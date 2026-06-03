"""
Pipeline de extração de dados de Termos de Securitização de CRIs.

Fluxo:
  Google Drive → download PDF → Two-Pass Gemini Architecture
    Passo 1: extração base (Termos + Cronograma)
    Passo 2: mapeamento de referências cruzadas via Python/Regex
    Passo 3: segunda chamada Gemini para extrair textos das cláusulas referenciadas
    Passo 4: enriquecimento dos Termos Definidos via Python
  → embeddings → persistência no Supabase → limpeza local
"""

import argparse
import json
import logging
import os
import re
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

# Passo 1: extração base — sem instrução de cross-reference (Python resolve)
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
   - O valor DEVE ser uma tabela Markdown com quebras de linha escapadas (\\n), neste formato:
     | Termo | Descrição |\\n|---|---|\\n| Termo 1 | Descrição 1 |\\n| Termo 2 | Descrição 2 |
   - Corrija todos os erros de OCR nos termos e nas descrições antes de montar a tabela.
   - Se um termo remeter a outra cláusula (ex: "Tem o significado previsto na Cláusula 3.10"),
     mantenha essa referência intacta no texto — ela será resolvida em etapa posterior.
   - Se a seção não for encontrada, retorne apenas: | Termo | Descrição |\\n|---|---|

4. CRONOGRAMA DE PAGAMENTOS — EXTRAÇÃO COMPLETA DE TODAS AS SÉRIES:
   - ATENÇÃO: Este documento pode conter múltiplas séries de CRI (ex: 178ª e 179ª Séries).
   - Você é OBRIGADO a localizar e extrair o cronograma de TODAS as séries. Não pare na primeira.
   - Formate com cabeçalho Markdown por série e \\n entre linhas. Exemplo:
     ### Cronograma — 178ª Série\\n| Data | Amortização | Juros | Total |\\n|---|---|---|---|\\n| ... |\\n\\n### Cronograma — 179ª Série\\n| Data | Amortização | Juros | Total |\\n|---|---|---|---|\\n| ... |
   - Se não encontrar nenhum cronograma, retorne: "Cronograma não localizado no documento."

================================================================================
🚨 REGRA CRÍTICA DE JSON — VIOLAÇÃO DESTA REGRA INUTILIZA TODO O RESULTADO
================================================================================

A sua resposta deve ser um JSON válido e perfeitamente parseável por json.loads().
- É ESTRITAMENTE PROIBIDO inserir quebras de linha LITERAIS dentro dos valores de texto.
  Qualquer quebra de linha DEVE ser escapada como \\n.
- Qualquer aspa dupla dentro de um valor DEVE ser escapada como \\".
- Retorne APENAS o JSON puro, sem bloco de código markdown ao redor (sem ```json).

Exemplo CORRETO  → "termos_definidos": "| Termo | Descrição |\\n|---|---|\\n| A | B |"
Exemplo ERRADO   → "termos_definidos": "| Termo | Descrição |
                                        |---|---|
                                        | A | B |"

================================================================================

REGRAS OBRIGATÓRIAS:
1. Retorne SOMENTE o objeto JSON válido, sem blocos markdown, sem texto explicativo.
2. Preserve TODAS as chaves do schema, mesmo que o valor seja nulo.
3. Use null para strings/datas não encontradas e 0 para números não encontrados.
4. Datas devem estar no formato YYYY-MM-DD.
5. taxa_spread deve ser o valor decimal puro (ex.: 2.5 para "2,5% a.a.").
6. Se houver múltiplas séries, liste todas no array "series".

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

# Passo 3: prompt focado na extração das cláusulas referenciadas
PROMPT_CLAUSULAS_TEMPLATE = """\
O documento em anexo é um Termo de Securitização de CRI (contrato financeiro em português).

Extraia o texto integral ESTRITAMENTE das seguintes cláusulas: {lista_clausulas}

Regras:
- Retorne SOMENTE o JSON, sem markdown ao redor.
- Para cada cláusula, copie o texto completo como aparece no documento, corrigindo OCR.
- Se uma cláusula não for encontrada, use o valor: "Cláusula não localizada no documento."

🚨 REGRA CRÍTICA DE JSON: Qualquer quebra de linha DEVE ser escapada como \\n.
   Qualquer aspa dupla DEVE ser escapada como \\".

Formato obrigatório:
{{
  "Cláusula 3.10": "texto integral da cláusula...",
  "Cláusula 5.1": "texto integral da cláusula..."
}}
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
# Two-Pass Architecture — funções auxiliares
# ---------------------------------------------------------------------------


def _sanitizar_json(texto_bruto: str) -> str:
    """Remove wrappers markdown e extrai a substring {…} do texto bruto."""
    texto = (
        texto_bruto.strip()
        .removeprefix("```json")
        .removeprefix("```")
        .removesuffix("```")
        .strip()
    )
    inicio = texto.find("{")
    fim = texto.rfind("}")
    if inicio != -1 and fim != -1:
        return texto[inicio : fim + 1]
    return texto


def _mapear_referencias_cruzadas(termos_texto: str) -> list[str]:
    """
    Usa regex para encontrar referências a cláusulas no texto de Termos Definidos.
    Retorna lista de referências únicas preservando a primeira ocorrência.
    """
    if not termos_texto:
        return []
    matches = re.findall(r"[Cc]l[aá]usula\s+\d+(?:\.\d+)*", termos_texto)
    seen: set[str] = set()
    unicas: list[str] = []
    for m in matches:
        # normaliza capitalização para deduplicação
        chave = m.strip().lower()
        if chave not in seen:
            seen.add(chave)
            # preserva capitalização original da primeira ocorrência
            unicas.append(m.strip())
    return unicas


def _extrair_clausulas_referenciadas(
    pdf_bytes: bytes, clausulas: list[str]
) -> dict[str, str]:
    """
    Passo 3: chama o Gemini enviando o PDF e pedindo apenas os textos
    das cláusulas identificadas no Passo 2.
    """
    lista_str = ", ".join(clausulas)
    prompt = PROMPT_CLAUSULAS_TEMPLATE.format(lista_clausulas=lista_str)

    model = genai.GenerativeModel("gemini-2.5-flash-lite")
    resposta = model.generate_content(
        [{"mime_type": "application/pdf", "data": pdf_bytes}, prompt],
        generation_config=genai.GenerationConfig(
            response_mime_type="application/json",
            temperature=0.0,
        ),
    )

    texto_limpo = _sanitizar_json(resposta.text or "")
    try:
        return json.loads(texto_limpo, strict=False)
    except json.JSONDecodeError as exc:
        logger.warning(
            "Passo 3: JSON das cláusulas inválido, retornando vazio. Erro: %s", exc
        )
        return {}


def _enriquecer_termos_definidos(
    termos_texto: str, clausulas_map: dict[str, str]
) -> str:
    """
    Passo 4: para cada referência encontrada no termos_texto, substitui
    'Cláusula X' por 'Cláusula X: [Texto: <conteúdo>]'.
    Usa lookup case-insensitive para tolerar variações de capitalização.
    """
    if not clausulas_map:
        return termos_texto

    # Índice normalizado para lookup rápido
    indice = {k.strip().lower(): v for k, v in clausulas_map.items()}

    def substituir(match: re.Match) -> str:
        referencia = match.group(0)
        texto_clausula = indice.get(referencia.strip().lower())
        if texto_clausula and texto_clausula != "Cláusula não localizada no documento.":
            return f"{referencia}: [Texto: {texto_clausula}]"
        if texto_clausula == "Cláusula não localizada no documento.":
            return f"{referencia} [não localizada no documento]"
        return referencia

    return re.sub(r"[Cc]l[aá]usula\s+\d+(?:\.\d+)*", substituir, termos_texto)


# ---------------------------------------------------------------------------
# Extração estruturada — Two-Pass Architecture
# ---------------------------------------------------------------------------


def _extrair_dados_gemini(caminho_pdf: str) -> dict:
    """
    Executa a arquitetura de dois passes para extração robusta de dados.

    Passo 1 — Extração base via Gemini (Termos + Cronograma).
    Passo 2 — Python/Regex mapeia referências cruzadas nos Termos Definidos.
    Passo 3 — Segunda chamada Gemini extrai textos das cláusulas referenciadas.
    Passo 4 — Python injeta os textos resolvidos nos Termos Definidos.
    """
    logger.info("Lendo PDF para envio inline ao Gemini: '%s'...", caminho_pdf)
    pdf_bytes = Path(caminho_pdf).read_bytes()
    logger.info("PDF carregado (%.1f MB).", len(pdf_bytes) / 1_048_576)

    model = genai.GenerativeModel("gemini-2.5-flash-lite")
    gen_config = genai.GenerationConfig(
        response_mime_type="application/json", temperature=0.0
    )

    # ── Passo 1: Extração base ──────────────────────────────────────────────
    logger.info("[Passo 1/4] Iniciando extração base (Termos Definidos + Cronograma)...")
    resposta = model.generate_content(
        [{"mime_type": "application/pdf", "data": pdf_bytes}, PROMPT_EXTRACAO],
        generation_config=gen_config,
    )

    texto_limpo = _sanitizar_json(resposta.text or "")
    try:
        dados = json.loads(texto_limpo, strict=False)
    except json.JSONDecodeError as exc:
        logger.error("Passo 1: JSON inválido. Trecho: %s", texto_limpo[:500])
        raise ValueError(f"Gemini retornou JSON inválido: {exc}") from exc

    n_series = len(dados.get("series", []))
    logger.info(
        "[Passo 1/4] Concluído: %d série(s) extraída(s).", n_series
    )

    # ── Passo 2: Mapeamento de referências cruzadas via Python/Regex ────────
    logger.info("[Passo 2/4] Mapeando referências cruzadas nos Termos Definidos...")
    termos_texto = dados.get("clausulas", {}).get("termos_definidos", "")
    clausulas_referenciadas = _mapear_referencias_cruzadas(termos_texto)

    if not clausulas_referenciadas:
        logger.info(
            "[Passo 2/4] Nenhuma referência cruzada identificada. Pulando Passos 3 e 4."
        )
        return dados

    logger.info(
        "[Passo 2/4] Identificadas %d cláusula(s) para busca: %s",
        len(clausulas_referenciadas),
        clausulas_referenciadas,
    )

    # ── Passo 3: Segunda chamada ao Gemini para extrair cláusulas ───────────
    logger.info(
        "[Passo 3/4] Iniciando extração das %d cláusula(s) referenciada(s)...",
        len(clausulas_referenciadas),
    )
    clausulas_map = _extrair_clausulas_referenciadas(pdf_bytes, clausulas_referenciadas)
    n_encontradas = sum(
        1 for v in clausulas_map.values()
        if v and v != "Cláusula não localizada no documento."
    )
    logger.info(
        "[Passo 3/4] Concluído: %d/%d cláusula(s) localizada(s) no documento.",
        n_encontradas,
        len(clausulas_referenciadas),
    )

    # ── Passo 4: Enriquecimento dos Termos Definidos via Python ─────────────
    logger.info("[Passo 4/4] Enriquecendo Termos Definidos com textos resolvidos...")
    dados["clausulas"]["termos_definidos"] = _enriquecer_termos_definidos(
        termos_texto, clausulas_map
    )
    logger.info("[Passo 4/4] Termos Definidos enriquecidos com sucesso.")

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

        for i, serie in enumerate(series_raw, start=1):
            session.add(CRISerie(
                cri_id=cri.id,
                nome_serie=serie.get("nome_serie") or None,
                data_vencimento=_parse_data(serie.get("data_vencimento")),
                indexador=serie.get("indexador") or None,
                taxa_spread=_parse_decimal(serie.get("taxa_spread")),
            ))
            logger.debug("CRISerie %d/%d adicionada.", i, len(series_raw))

        for chave in CLAUSULAS_KEYS:
            texto = clausulas_raw.get(chave) or ""
            embedding = _gerar_embedding(texto)
            session.add(CRIClausula(
                cri_id=cri.id,
                tipo_clausula=chave,
                texto_original=texto or None,
                embedding=embedding,
            ))
            logger.debug("CRIClausula '%s' adicionada (embedding=%s).", chave, embedding is not None)

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
        logger.info("[1/3] Localizando e baixando PDF do Google Drive...")
        drive_service = obter_servico()
        file_id = encontrar_termo_por_codigo_if(drive_service, codigo_if)

        if not file_id:
            logger.error(
                "Termo de Securitização não encontrado no Drive para '%s'.", codigo_if
            )
            return

        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        caminho_pdf = baixar_pdf_drive(drive_service, file_id, str(TEMP_DIR))
        logger.info("[1/3] PDF disponível em: '%s'.", caminho_pdf)

        logger.info("[2/3] Executando Two-Pass Architecture com Gemini...")
        dados = _extrair_dados_gemini(caminho_pdf)

        logger.info("[3/3] Gerando embeddings e persistindo no Supabase...")
        _persistir_dados(codigo_if, dados)

        logger.info("╚══ Pipeline concluído com sucesso para '%s'. ══╝", codigo_if)

    except Exception as exc:
        logger.exception("Erro irrecuperável no pipeline para '%s': %s", codigo_if, exc)
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
            "processa com Gemini (Two-Pass) e persiste no Supabase."
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
