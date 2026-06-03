"""
Pipeline de extração de dados de Termos de Securitização de CRIs.

Fluxo:
  Google Drive → download PDF → Gemini (inline) → extração JSON estruturada
  → embeddings → persistência no Supabase → limpeza local
"""

import argparse
import json
import logging
import os
import sys
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

CLAUSULAS_KEYS = [
    "termos_definidos",
    "cronograma_pagamentos",
]

PROMPT_EXTRACAO = """\
Você é um especialista em instrumentos financeiros brasileiros de renda fixa, com profundo \
conhecimento em Certificados de Recebíveis Imobiliários (CRIs).

Analise o Termo de Securitização anexo e extraia as informações com máxima fidelidade ao \
documento original.

================================================================================
⚠️  DIRETIVAS RIGOROSAS DE OCR E FORMATAÇÃO — LEIA ANTES DE QUALQUER OUTRA COISA
================================================================================

1. É ESTRITAMENTE PROIBIDO O USO DE LATEX OU CÓDIGOS MATEMÁTICOS DE QUALQUER TIPO.
   Nunca escreva sequências como \\c, ~o, \\ca~o, ^{}, \\text{}, $...$ ou similares.

2. O documento está em português do Brasil. Você deve atuar como um CORRETOR ORTOGRÁFICO:
   - ac\\co~es        → ações
   - instituic\\ca~o  → instituição
   - \\ca~o           → ção
   - emissa~o         → emissão
   - obrigac\\co~es   → obrigações
   Aplique esta correção em TODOS os campos do JSON, sem exceção.

3. PROIBIÇÃO ABSOLUTA para a cláusula "termos_definidos":
   - É PROIBIDO retornar texto corrido neste campo.
   - O valor DEVE ser uma tabela Markdown com quebras de linha reais (\\n), neste formato exato:
     | Termo | Descrição |\\n|---|---|\\n| Termo 1 | Descrição 1 |\\n| Termo 2 | Descrição 2 |
   - Corrija todos os erros de OCR nos termos e nas descrições antes de montar a tabela.
   - Se a seção não for encontrada, retorne apenas: | Termo | Descrição |\\n|---|---|

4. RESOLUÇÃO DE REFERÊNCIA CRUZADA (termos_definidos):
   - Se a definição de um termo remeter a outra cláusula do documento
     (ex: "Tem o significado previsto na Cláusula 3.10" ou "conforme descrito na Cláusula 5"),
     você é OBRIGADO a localizar o conteúdo dessa cláusula específica no restante do documento
     e adicioná-lo à descrição entre colchetes.
   - Formato obrigatório:
     Tem o significado previsto na Cláusula 3.10 [Texto completo ou resumo fiel da Cláusula 3.10
     encontrada no documento].
   - JAMAIS deixe uma referência cruzada sem resolução. O usuário não tem acesso ao documento
     original e depende inteiramente do contexto que você fornecer.

5. CRONOGRAMA DE PAGAMENTOS — EXTRAÇÃO COMPLETA DE TODAS AS SÉRIES:
   - ATENÇÃO: Este documento pode conter múltiplas séries de CRI (ex: 178ª e 179ª Séries).
   - Você é OBRIGADO a localizar e extrair o cronograma de pagamentos de TODAS as séries
     presentes no documento. Não pare na primeira série encontrada.
   - Formate a resposta estruturando cada série com um cabeçalho Markdown seguido da sua
     respectiva tabela, usando \\n entre as linhas. Exemplo obrigatório:
     ### Cronograma — 178ª Série\\n| Data | Amortização | Juros | Total |\\n|---|---|---|---|\\n| ... |\\n\\n### Cronograma — 179ª Série\\n| Data | Amortização | Juros | Total |\\n|---|---|---|---|\\n| ... |
   - Se não encontrar nenhum cronograma, retorne: "Cronograma não localizado no documento."

================================================================================

REGRAS OBRIGATÓRIAS:
1. Retorne SOMENTE o objeto JSON válido, sem blocos markdown, sem texto explicativo.
2. Preserve TODAS as chaves do schema, mesmo que o valor seja nulo.
3. Use null para strings/datas não encontradas e 0 para números não encontrados.
4. Datas devem estar no formato YYYY-MM-DD.
5. taxa_spread deve ser o valor decimal puro (ex.: 2.5 para "2,5% a.a.").
6. Se houver múltiplas séries, liste todas no array "series".

================================================================================
🚨 REGRA CRÍTICA DE JSON — VIOLAÇÃO DESTA REGRA INUTILIZA TODO O RESULTADO
================================================================================

A sua resposta deve ser um JSON válido e perfeitamente parseável por json.loads().
- É ESTRITAMENTE PROIBIDO inserir quebras de linha LITERAIS (reais) dentro dos valores
  de texto. Qualquer quebra de linha — incluindo as de tabelas Markdown — DEVE ser
  obrigatoriamente escapada como \\n (barra-n).
- Qualquer aspa dupla dentro de um valor de texto DEVE ser escapada como \\".
- Retorne APENAS o JSON puro, sem bloco de código markdown ao redor (sem ```json).

Exemplo CORRETO  → "termos_definidos": "| Termo | Descrição |\\n|---|---|\\n| A | B |"
Exemplo ERRADO   → "termos_definidos": "| Termo | Descrição |
                                        |---|---|
                                        | A | B |"

================================================================================

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
    "termos_definidos": "| Termo | Descrição |\\n|---|---|\\n| ... | ... |",
    "cronograma_pagamentos": "### Cronograma — Xª Série\\n| col | col |\\n|---|---|\\n| ... |"
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
# Extração estruturada (PDF inline — sem File API)
# ---------------------------------------------------------------------------


def _extrair_dados_gemini(caminho_pdf: str) -> dict:
    """
    Lê o PDF localmente e envia inline para o gemini-2.5-flash-lite.

    Evita a File API (que exige permissões adicionais na chave).
    Limite prático: PDFs até ~20 MB.
    """
    logger.info("Lendo PDF para envio inline ao Gemini: '%s'...", caminho_pdf)
    pdf_bytes = Path(caminho_pdf).read_bytes()
    tamanho_mb = len(pdf_bytes) / 1_048_576
    logger.info("PDF carregado (%.1f MB). Solicitando extração estruturada...", tamanho_mb)

    model = genai.GenerativeModel("gemini-2.5-flash-lite")

    resposta = model.generate_content(
        [{"mime_type": "application/pdf", "data": pdf_bytes}, PROMPT_EXTRACAO],
        generation_config=genai.GenerationConfig(
            response_mime_type="application/json",
            temperature=0.0,  # máximo determinismo para extração factual
        ),
    )

    texto_bruto = resposta.text or ""

    # Sanitização 1: remove blocos de código markdown que o modelo pode inserir
    texto_limpo = (
        texto_bruto.strip()
        .removeprefix("```json")
        .removeprefix("```")
        .removesuffix("```")
        .strip()
    )

    # Sanitização 2: localiza a substring JSON (do primeiro { ao último })
    inicio = texto_limpo.find("{")
    fim = texto_limpo.rfind("}")
    if inicio != -1 and fim != -1:
        texto_limpo = texto_limpo[inicio : fim + 1]

    try:
        # strict=False aceita caracteres de controle não escapados (ex: \n literal)
        # como último recurso, evitando falha total na extração
        dados = json.loads(texto_limpo, strict=False)
    except json.JSONDecodeError as exc:
        trecho = texto_limpo[:500] if texto_limpo else "<vazio>"
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
    Gera vetor de 768 dimensões via embedding-001.

    Retorna None se o texto for vazio ou a chamada falhar (não bloqueia o pipeline).
    """
    if not texto or not texto.strip():
        return None
    try:
        resultado = genai.embed_content(
            model="models/embedding-001",
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
      - 2 registros CRIClausula (termos_definidos + cronograma_pagamentos)
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

        # 3. CRIClausula + embeddings (apenas termos_definidos e cronograma)
        for chave in CLAUSULAS_KEYS:
            texto = clausulas_raw.get(chave) or ""
            embedding = _gerar_embedding(texto)

            if embedding:
                logger.debug("Embedding gerado para '%s' (%d dims).", chave, len(embedding))
            else:
                logger.debug("'%s' sem embedding (texto vazio ou erro).", chave)

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

    Garante limpeza do PDF local no bloco finally, mesmo em caso de falha.
    """
    logger.info("╔══ Pipeline iniciado para código IF: '%s' ══╗", codigo_if)

    _configurar_gemini()

    caminho_pdf: str | None = None

    try:
        # ── Etapa 1: Localização e download ─────────────────────────────────
        logger.info("[1/4] Localizando PDF no Google Drive...")
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
        logger.info("[1/4] PDF disponível em: '%s'.", caminho_pdf)

        # ── Etapa 2: Extração estruturada (PDF inline) ───────────────────────
        logger.info("[2/4] Extraindo dados estruturados com gemini-2.5-flash-lite...")
        dados = _extrair_dados_gemini(caminho_pdf)

        # ── Etapas 3 + 4: Embeddings + persistência ──────────────────────────
        logger.info("[3-4/4] Gerando embeddings e persistindo no Supabase...")
        _persistir_dados(codigo_if, dados)

        logger.info("╚══ Pipeline concluído com sucesso para '%s'. ══╝", codigo_if)

    except Exception as exc:
        logger.exception(
            "Erro irrecuperável no pipeline para '%s': %s", codigo_if, exc
        )
        raise

    finally:
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
        epilog="Exemplo: python pipeline_extracao.py 19K1139273",
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
