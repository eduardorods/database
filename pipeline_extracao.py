"""
Pipeline de extração de dados de Termos de Securitização de CRIs.

Fluxo:
  Google Drive → download PDF → Gemini File API → extração JSON estruturada
  → embeddings por cláusula → persistência no Supabase → limpeza de recursos
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import google.generativeai as genai
from dotenv import load_dotenv

from database import SessionLocal
from drive_utils import baixar_pdf_drive, encontrar_termo_por_codigo_if, obter_servico
from models import CRIClausula, CRIMetadata, CRISerie

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TEMP_DIR = Path("./temp")

# Chaves das cláusulas — ordem importa para logs e inserção
CLAUSULAS_KEYS = [
    "fundo_reserva",
    "fundo_despesa",
    "covenants",
    "garantias",
    "amortizacao",
    "cronograma_pagamentos",
    "termos_definidos",
]

PROMPT_EXTRACAO = """\
Você é um especialista em instrumentos financeiros brasileiros de renda fixa, com profundo \
conhecimento em Certificados de Recebíveis Imobiliários (CRIs).

Analise o Termo de Securitização anexo e extraia as informações com máxima fidelidade ao \
documento original.

REGRAS OBRIGATÓRIAS:
1. Retorne SOMENTE o objeto JSON válido, sem blocos markdown, sem texto explicativo.
2. Preserve TODAS as chaves do schema, mesmo que o valor seja nulo.
3. Use null para strings/datas não encontradas e 0 para números não encontrados.
4. Datas devem estar no formato YYYY-MM-DD.
5. taxa_spread deve ser o valor decimal puro (ex.: 2.5 para "2,5% a.a.").
6. Para o bloco "clausulas", transcreva o texto LITERAL do documento; não resuma nem parafraseie.
7. cronograma_pagamentos deve ser formatado como tabela Markdown (| col | col |).
8. Se houver múltiplas séries, liste todas no array "series".

Schema esperado:
{
  "metadata": {
    "securitizadora": "",
    "numero_emissao": 0,
    "agente_fiduciario": "",
    "devedor": "",
    "data_emissao": "YYYY-MM-DD",
    "frequencia_juros": "",
    "data_inicio_juros": "YYYY-MM-DD",
    "frequencia_amortizacao": ""
  },
  "series": [
    {
      "nome_serie": "",
      "data_vencimento": "YYYY-MM-DD",
      "indexador": "",
      "taxa_spread": 0.0
    }
  ],
  "clausulas": {
    "fundo_reserva": "texto literal do documento",
    "fundo_despesa": "texto literal do documento",
    "covenants": "texto literal",
    "garantias": "texto literal e pormenorizado",
    "amortizacao": "texto literal",
    "cronograma_pagamentos": "tabela em formato markdown",
    "termos_definidos": "glossário completo extraído literalmente"
  }
}
"""


# ---------------------------------------------------------------------------
# Configuração e autenticação
# ---------------------------------------------------------------------------


def _configurar_gemini() -> None:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Variável de ambiente GEMINI_API_KEY não definida. Verifique seu arquivo .env."
        )
    genai.configure(api_key=api_key)
    logger.debug("SDK google-generativeai configurado.")


# ---------------------------------------------------------------------------
# Gemini File API
# ---------------------------------------------------------------------------


def _fazer_upload_pdf(caminho_pdf: str):
    """
    Envia o PDF para a File API do Gemini e aguarda o processamento.

    O polling tem timeout de 60 s — suficiente para PDFs de até ~50 MB.
    """
    logger.info("Enviando PDF para a File API do Gemini: '%s'...", caminho_pdf)
    arquivo = genai.upload_file(path=caminho_pdf, mime_type="application/pdf")
    logger.info(
        "Upload concluído (name='%s'). Aguardando processamento...", arquivo.name
    )

    tentativas = 0
    max_tentativas = 30  # 30 × 2 s = 60 s
    while arquivo.state.name == "PROCESSING":
        if tentativas >= max_tentativas:
            raise TimeoutError(
                f"Arquivo '{arquivo.name}' não ficou pronto após {max_tentativas * 2}s."
            )
        time.sleep(2)
        arquivo = genai.get_file(arquivo.name)
        tentativas += 1

    if arquivo.state.name == "FAILED":
        raise RuntimeError(
            f"Processamento do arquivo '{arquivo.name}' falhou no servidor Gemini."
        )

    logger.info("Arquivo pronto para uso (state='%s').", arquivo.state.name)
    return arquivo


# ---------------------------------------------------------------------------
# Extração estruturada
# ---------------------------------------------------------------------------


def _extrair_dados_gemini(arquivo_gemini) -> dict:
    """Chama gemini-1.5-pro e retorna o dict extraído do PDF."""
    logger.info("Solicitando extração estruturada ao modelo gemini-1.5-pro...")
    model = genai.GenerativeModel("gemini-1.5-pro")

    resposta = model.generate_content(
        [arquivo_gemini, PROMPT_EXTRACAO],
        generation_config=genai.GenerationConfig(
            response_mime_type="application/json",
            temperature=0.0,  # máximo determinismo para extração factual
        ),
    )

    try:
        dados = json.loads(resposta.text)
    except json.JSONDecodeError as exc:
        trecho = resposta.text[:500] if resposta.text else "<vazio>"
        logger.error("Resposta do Gemini não é JSON válido. Trecho: %s", trecho)
        raise ValueError(f"Gemini retornou JSON inválido: {exc}") from exc

    n_series = len(dados.get("series", []))
    n_clausulas = len([v for v in dados.get("clausulas", {}).values() if v])
    logger.info(
        "Extração concluída: %d série(s), %d cláusula(s) com conteúdo.",
        n_series,
        n_clausulas,
    )
    return dados


# ---------------------------------------------------------------------------
# Embeddings
# ---------------------------------------------------------------------------


def _gerar_embedding(texto: str) -> list[float] | None:
    """
    Gera vetor de 768 dimensões via text-embedding-004.

    Retorna None se o texto for vazio ou a chamada falhar (não bloqueia o pipeline).
    """
    if not texto or not texto.strip():
        return None
    try:
        resultado = genai.embed_content(
            model="models/text-embedding-004",
            content=texto,
            task_type="RETRIEVAL_DOCUMENT",
        )
        return resultado["embedding"]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Falha ao gerar embedding (texto truncado: '%.60s...'): %s", texto, exc)
        return None


# ---------------------------------------------------------------------------
# Helpers de conversão
# ---------------------------------------------------------------------------


def _parse_data(valor: str | None) -> date | None:
    if not valor or valor in ("YYYY-MM-DD", "null", ""):
        return None
    try:
        return date.fromisoformat(valor)
    except ValueError:
        logger.warning("Data com formato inesperado ignorada: '%s'.", valor)
        return None


def _parse_decimal(valor) -> Decimal | None:
    if valor is None or valor == 0:
        return None
    try:
        return Decimal(str(valor))
    except Exception:  # noqa: BLE001
        logger.warning("Valor de taxa inválido ignorado: '%s'.", valor)
        return None


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------


def _persistir_dados(codigo_if: str, dados: dict) -> None:
    """
    Salva em uma única transação:
      - 1 registro CRIMetadata
      - N registros CRISerie
      - 7 registros CRIClausula (com embeddings quando disponíveis)
    """
    meta = dados.get("metadata", {})
    series_raw = dados.get("series", [])
    clausulas_raw = dados.get("clausulas", {})

    logger.info("Iniciando persistência para código IF '%s'...", codigo_if)

    with SessionLocal() as session:
        # 1. CRIMetadata
        cri = CRIMetadata(
            codigo_if=codigo_if,
            securitizadora=meta.get("securitizadora") or None,
            numero_emissao=int(meta["numero_emissao"]) if meta.get("numero_emissao") else None,
            agente_fiduciario=meta.get("agente_fiduciario") or None,
            devedor=meta.get("devedor") or None,
            data_emissao=_parse_data(meta.get("data_emissao")),
            frequencia_juros=meta.get("frequencia_juros") or None,
            data_inicio_juros=_parse_data(meta.get("data_inicio_juros")),
            frequencia_amortizacao=meta.get("frequencia_amortizacao") or None,
        )
        session.add(cri)
        # flush para obter cri.id antes de criar os filhos
        session.flush()
        logger.debug("CRIMetadata criado com id='%s'.", cri.id)

        # 2. CRISerie
        for i, serie in enumerate(series_raw, start=1):
            obj_serie = CRISerie(
                cri_id=cri.id,
                nome_serie=serie.get("nome_serie") or None,
                data_vencimento=_parse_data(serie.get("data_vencimento")),
                indexador=serie.get("indexador") or None,
                taxa_spread=_parse_decimal(serie.get("taxa_spread")),
            )
            session.add(obj_serie)
            logger.debug("CRISerie %d/%d adicionada: '%s'.", i, len(series_raw), obj_serie.nome_serie)

        # 3. CRIClausula + embeddings
        for chave in CLAUSULAS_KEYS:
            texto = clausulas_raw.get(chave) or ""
            embedding = _gerar_embedding(texto)

            if embedding:
                logger.debug(
                    "Embedding gerado para cláusula '%s' (%d dims).", chave, len(embedding)
                )
            else:
                logger.debug("Cláusula '%s' sem embedding (texto vazio ou erro).", chave)

            session.add(
                CRIClausula(
                    cri_id=cri.id,
                    tipo_clausula=chave,
                    texto_original=texto or None,
                    embedding=embedding,
                )
            )

        session.commit()

    logger.info(
        "Persistência concluída: 1 CRIMetadata | %d CRISerie | %d CRIClausula.",
        len(series_raw),
        len(CLAUSULAS_KEYS),
    )


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------


def executar_pipeline(codigo_if: str) -> None:
    """
    Executa o pipeline completo de extração para um código IF.

    Garante limpeza de recursos (arquivo Gemini + PDF local) no bloco finally,
    mesmo em caso de falha em qualquer etapa intermediária.
    """
    logger.info("╔══ Pipeline iniciado para código IF: '%s' ══╗", codigo_if)

    _configurar_gemini()

    caminho_pdf: str | None = None
    arquivo_gemini = None

    try:
        # ── Etapa 1: Localização e download ─────────────────────────────────
        logger.info("[1/5] Localizando PDF no Google Drive...")
        drive_service = obter_servico()
        file_id = encontrar_termo_por_codigo_if(drive_service, codigo_if)

        if not file_id:
            logger.error(
                "Termo de Securitização não encontrado no Drive para '%s'. "
                "Verifique se a pasta e o arquivo existem.",
                codigo_if,
            )
            return

        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        caminho_pdf = baixar_pdf_drive(drive_service, file_id, str(TEMP_DIR))
        logger.info("[1/5] PDF disponível em: '%s'.", caminho_pdf)

        # ── Etapa 2: Upload para a File API do Gemini ────────────────────────
        logger.info("[2/5] Fazendo upload para a File API do Gemini...")
        arquivo_gemini = _fazer_upload_pdf(caminho_pdf)

        # ── Etapa 3: Extração estruturada ────────────────────────────────────
        logger.info("[3/5] Extraindo dados estruturados com gemini-1.5-pro...")
        dados = _extrair_dados_gemini(arquivo_gemini)

        # ── Etapas 4 + 5: Embeddings + persistência ──────────────────────────
        # Os embeddings são gerados dentro de _persistir_dados, por cláusula,
        # para evitar carregar todos em memória antes de abrir a transação.
        logger.info("[4-5/5] Gerando embeddings e persistindo no Supabase...")
        _persistir_dados(codigo_if, dados)

        logger.info("╚══ Pipeline concluído com sucesso para '%s'. ══╝", codigo_if)

    except Exception as exc:
        logger.exception(
            "Erro irrecuperável no pipeline para '%s': %s", codigo_if, exc
        )
        raise

    finally:
        # ── Limpeza segura ────────────────────────────────────────────────────
        logger.info("Iniciando limpeza de recursos temporários...")

        if arquivo_gemini is not None:
            try:
                genai.delete_file(arquivo_gemini.name)
                logger.info("Arquivo removido do servidor Gemini: '%s'.", arquivo_gemini.name)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Não foi possível deletar o arquivo Gemini '%s': %s",
                    arquivo_gemini.name,
                    exc,
                )

        if caminho_pdf is not None:
            pdf_path = Path(caminho_pdf)
            if pdf_path.exists():
                try:
                    pdf_path.unlink()
                    logger.info("PDF local removido: '%s'.", caminho_pdf)
                except OSError as exc:
                    logger.warning("Não foi possível remover o PDF local '%s': %s", caminho_pdf, exc)


# ---------------------------------------------------------------------------
# Entrypoint CLI
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Extrai dados de um Termo de Securitização de CRI do Google Drive, "
            "processa com Gemini e persiste no Supabase."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Exemplo: python pipeline_extracao.py CRI12345678",
    )
    parser.add_argument(
        "codigo_if",
        type=str,
        help="Código IF do CRI — deve corresponder exatamente ao nome da pasta no Drive.",
    )
    args = parser.parse_args()

    try:
        executar_pipeline(args.codigo_if)
    except Exception:
        sys.exit(1)
