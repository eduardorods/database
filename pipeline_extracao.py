"""
Pipeline de extração de dados de Termos de Securitização de CRIs.

Fluxo:
  Google Drive → download em lote (TS + Aditamentos + Atas)
  → Two-Pass Gemini Architecture (Termos + Cronograma + Cross-reference)
  → Extração de Eventos (Aditamentos e Atas)
  → Embeddings → Persistência no Supabase → Limpeza local
"""

import argparse
import json
import logging
import os
import re
import sys
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

import google.generativeai as genai
from dotenv import load_dotenv

from database import SessionLocal
from drive_utils import baixar_documentos_cri, obter_servico
from models import CRIClausula, CRIEvento, CRIMetadata, CRISerie

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

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

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

# Passo 3 da Two-Pass Architecture: extração de cláusulas via XML/tags
PROMPT_CLAUSULAS_TEMPLATE = """\
O documento em anexo é um Termo de Securitização de CRI (contrato financeiro em português).

Extraia o texto integral ESTRITAMENTE das seguintes cláusulas: {lista_clausulas}

NÃO RETORNE JSON. Retorne OBRIGATORIAMENTE no seguinte formato de tags para cada cláusula:

<item id="Cláusula 3.10">TEXTO INTEGRAL DA CLÁUSULA AQUI</item>
<item id="Cláusula 5.1">TEXTO INTEGRAL DA CLÁUSULA AQUI</item>

Regras:
- Use exatamente o mesmo identificador recebido no campo id= (ex: "Cláusula 3.10").
- Copie o texto completo da cláusula, corrigindo erros de OCR.
- Se uma cláusula não for encontrada, use: <item id="Cláusula X.Y">Cláusula não localizada no documento.</item>
- Não adicione nenhum texto fora das tags <item>.
"""

# Sprint 4: extração de data e resumo de Aditamentos e Atas
PROMPT_EVENTO_TEMPLATE = """\
O documento em anexo é um(a) {tipo} de um Fundo/CRI em português do Brasil.

Leia o documento completo e retorne um JSON com exatamente duas chaves:
- "data_evento": a data em que o documento ocorreu ou foi assinado (formato DD/MM/YYYY ou por extenso).
- "resumo": um resumo detalhado e executivo das principais modificações, dispensas de covenants
  ou deliberações aprovadas. Seja específico: mencione valores, percentuais, prazos e partes
  envolvidas quando presentes.

🚨 REGRA CRÍTICA DE JSON: Qualquer quebra de linha DEVE ser escapada como \\n.
   Qualquer aspa dupla DEVE ser escapada como \\".
   Retorne APENAS o JSON puro, sem ```json ao redor.

Formato obrigatório:
{{
  "data_evento": "DD/MM/YYYY",
  "resumo": "Resumo executivo detalhado..."
}}
"""


# ---------------------------------------------------------------------------
# Configuração
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
# Helpers compartilhados
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


def _gerar_embedding(texto: str) -> list[float] | None:
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
        logger.warning("Falha ao gerar embedding ('%.60s...'): %s", texto, exc)
        return None


# ---------------------------------------------------------------------------
# Two-Pass Architecture — Termo de Securitização
# ---------------------------------------------------------------------------


def _mapear_referencias_cruzadas(termos_texto: str) -> list[str]:
    if not termos_texto:
        return []
    matches = re.findall(r"[Cc]l[aá]usula\s+\d+(?:\.\d+)*", termos_texto)
    seen: set[str] = set()
    unicas: list[str] = []
    for m in matches:
        chave = m.strip().lower()
        if chave not in seen:
            seen.add(chave)
            unicas.append(m.strip())
    return unicas


def _extrair_clausulas_referenciadas(
    pdf_bytes: bytes, clausulas: list[str]
) -> dict[str, str]:
    """Passo 3: segunda chamada Gemini via XML/tags — robusto para textos longos."""
    lista_str = ", ".join(clausulas)
    prompt = PROMPT_CLAUSULAS_TEMPLATE.format(lista_clausulas=lista_str)

    model = genai.GenerativeModel("gemini-2.5-flash-lite")
    resposta = model.generate_content(
        [{"mime_type": "application/pdf", "data": pdf_bytes}, prompt],
        generation_config=genai.GenerationConfig(temperature=0.0),
    )

    texto_resposta = resposta.text or ""
    clausulas_extraidas: dict[str, str] = {}
    for match in re.finditer(
        r'<item\s+id="(.*?)">(.*?)</item>', texto_resposta, re.DOTALL | re.IGNORECASE
    ):
        clausulas_extraidas[match.group(1).strip()] = match.group(2).strip()

    if not clausulas_extraidas:
        logger.warning(
            "Passo 3: nenhuma tag <item> encontrada. Trecho: %s", texto_resposta[:300]
        )
    return clausulas_extraidas


def _enriquecer_termos_definidos(
    termos_texto: str, clausulas_map: dict[str, str]
) -> str:
    if not clausulas_map:
        return termos_texto
    indice = {k.strip().lower(): v for k, v in clausulas_map.items()}

    def substituir(match: re.Match) -> str:
        ref = match.group(0)
        texto_clausula = indice.get(ref.strip().lower())
        if texto_clausula and texto_clausula != "Cláusula não localizada no documento.":
            return f"{ref}: [Texto: {texto_clausula}]"
        if texto_clausula == "Cláusula não localizada no documento.":
            return f"{ref} [não localizada no documento]"
        return ref

    return re.sub(r"[Cc]l[aá]usula\s+\d+(?:\.\d+)*", substituir, termos_texto)


def _extrair_dados_gemini(caminho_pdf: str) -> dict:
    """
    Two-Pass Architecture para extração do Termo de Securitização.

    Passo 1 — Extração base (Termos + Cronograma).
    Passo 2 — Python/Regex mapeia referências cruzadas.
    Passo 3 — Segunda chamada Gemini extrai textos das cláusulas referenciadas.
    Passo 4 — Python injeta os textos nos Termos Definidos.
    """
    logger.info("Lendo PDF: '%s'...", caminho_pdf)
    pdf_bytes = Path(caminho_pdf).read_bytes()
    logger.info("PDF carregado (%.1f MB).", len(pdf_bytes) / 1_048_576)

    model = genai.GenerativeModel("gemini-2.5-flash-lite")
    gen_config = genai.GenerationConfig(
        response_mime_type="application/json", temperature=0.0
    )

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
    logger.info("[Passo 1/4] Concluído: %d série(s) extraída(s).", len(dados.get("series", [])))

    logger.info("[Passo 2/4] Mapeando referências cruzadas nos Termos Definidos...")
    termos_texto = dados.get("clausulas", {}).get("termos_definidos", "")
    clausulas_referenciadas = _mapear_referencias_cruzadas(termos_texto)

    if not clausulas_referenciadas:
        logger.info("[Passo 2/4] Nenhuma referência cruzada. Pulando Passos 3 e 4.")
        return dados

    logger.info(
        "[Passo 2/4] Identificadas %d cláusula(s): %s",
        len(clausulas_referenciadas), clausulas_referenciadas,
    )

    logger.info("[Passo 3/4] Extraindo textos das cláusulas referenciadas...")
    clausulas_map = _extrair_clausulas_referenciadas(pdf_bytes, clausulas_referenciadas)
    n_ok = sum(1 for v in clausulas_map.values() if v != "Cláusula não localizada no documento.")
    logger.info("[Passo 3/4] %d/%d cláusula(s) localizada(s).", n_ok, len(clausulas_referenciadas))

    logger.info("[Passo 4/4] Enriquecendo Termos Definidos...")
    dados["clausulas"]["termos_definidos"] = _enriquecer_termos_definidos(
        termos_texto, clausulas_map
    )
    logger.info("[Passo 4/4] Termos Definidos enriquecidos com sucesso.")

    return dados


# ---------------------------------------------------------------------------
# Sprint 4 — Extração de Eventos (Aditamentos e Atas)
# ---------------------------------------------------------------------------


def _extrair_evento_gemini(caminho_pdf: str, tipo: str) -> dict:
    """
    Extrai data e resumo executivo de um Aditamento ou Ata de Assembleia.

    Args:
        caminho_pdf: caminho local do PDF.
        tipo: 'ADITAMENTO' ou 'ATA'.

    Returns:
        {'data_evento': '...', 'resumo': '...'}
    """
    logger.info("Extraindo evento '%s': '%s'...", tipo, Path(caminho_pdf).name)
    pdf_bytes = Path(caminho_pdf).read_bytes()
    prompt = PROMPT_EVENTO_TEMPLATE.format(tipo=tipo.lower().replace("_", " "))

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
        dados = json.loads(texto_limpo, strict=False)
        logger.info(
            "Evento extraído: data='%s', resumo=%.60s...",
            dados.get("data_evento"), dados.get("resumo", ""),
        )
        return dados
    except json.JSONDecodeError as exc:
        logger.warning(
            "Evento '%s': JSON inválido (%s). Retornando vazio.", caminho_pdf, exc
        )
        return {"data_evento": None, "resumo": None}


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------


def _persistir_dados(codigo_if: str, dados: dict) -> uuid.UUID:
    """
    Salva CRIMetadata, CRISerie e CRIClausula numa única transação.

    Returns:
        UUID do CRIMetadata criado (usado para vincular CRIEvento).
    """
    meta = dados.get("metadata", {})
    series_raw = dados.get("series", [])
    clausulas_raw = dados.get("clausulas", {})

    logger.info("Persistindo dados do TS para código IF '%s'...", codigo_if)

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

        session.commit()
        cri_id = cri.id

    logger.info(
        "TS persistido: 1 CRIMetadata | %d CRISerie | %d CRIClausula.",
        len(series_raw), len(CLAUSULAS_KEYS),
    )
    return cri_id


def _persistir_evento(
    cri_id: uuid.UUID, caminho_pdf: str, tipo: str, dados: dict
) -> None:
    """Salva um CRIEvento (Aditamento ou Ata) vinculado ao CRIMetadata."""
    nome_arquivo = Path(caminho_pdf).name
    with SessionLocal() as session:
        session.add(CRIEvento(
            cri_id=cri_id,
            tipo_documento=tipo,
            nome_arquivo=nome_arquivo,
            data_evento=dados.get("data_evento"),
            resumo=dados.get("resumo"),
        ))
        session.commit()
    logger.info("CRIEvento persistido: '%s' (%s).", nome_arquivo, tipo)


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------


def executar_pipeline(codigo_if: str) -> None:
    """
    Executa o pipeline completo para um código IF:

    1. Baixa todos os documentos do Drive (TS + Aditamentos + Atas).
    2. Extrai e persiste o Termo de Securitização (Two-Pass Architecture).
    3. Extrai e persiste cada Aditamento e Ata como CRIEvento.
    4. Remove todos os PDFs temporários no finally.
    """
    logger.info("╔══ Pipeline iniciado para código IF: '%s' ══╗", codigo_if)
    _configurar_gemini()

    documentos: dict = {"termo_principal": None, "aditamentos": [], "atas": []}

    try:
        # ── Etapa 1: Download em lote do Drive ──────────────────────────────
        logger.info("[1/3] Baixando documentos do Google Drive...")
        drive_service = obter_servico()
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        documentos = baixar_documentos_cri(drive_service, codigo_if, str(TEMP_DIR))

        if not documentos["termo_principal"]:
            logger.error("Termo de Securitização não encontrado para '%s'. Abortando.", codigo_if)
            return

        # ── Etapa 2: Two-Pass Architecture — Termo de Securitização ─────────
        logger.info("[2/3] Processando Termo de Securitização...")
        dados_ts = _extrair_dados_gemini(documentos["termo_principal"])
        cri_id = _persistir_dados(codigo_if, dados_ts)

        # ── Etapa 3: Extração de Eventos ────────────────────────────────────
        total_eventos = len(documentos["aditamentos"]) + len(documentos["atas"])
        logger.info("[3/3] Processando %d evento(s) (Aditamentos + Atas)...", total_eventos)

        for caminho in documentos["aditamentos"]:
            try:
                dados_evento = _extrair_evento_gemini(caminho, "ADITAMENTO")
                _persistir_evento(cri_id, caminho, "ADITAMENTO", dados_evento)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Falha ao processar aditamento '%s': %s", caminho, exc)

        for caminho in documentos["atas"]:
            try:
                dados_evento = _extrair_evento_gemini(caminho, "ATA")
                _persistir_evento(cri_id, caminho, "ATA", dados_evento)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Falha ao processar ata '%s': %s", caminho, exc)

        logger.info("╚══ Pipeline concluído com sucesso para '%s'. ══╝", codigo_if)

    except Exception as exc:
        logger.exception("Erro irrecuperável no pipeline para '%s': %s", codigo_if, exc)
        raise

    finally:
        # Remove todos os PDFs temporários baixados
        todos_caminhos = [documentos["termo_principal"]] if documentos["termo_principal"] else []
        todos_caminhos += documentos.get("aditamentos", [])
        todos_caminhos += documentos.get("atas", [])
        for caminho in todos_caminhos:
            try:
                Path(caminho).unlink(missing_ok=True)
                logger.info("PDF removido: '%s'.", caminho)
            except OSError as exc:
                logger.warning("Não foi possível remover '%s': %s", caminho, exc)


# ---------------------------------------------------------------------------
# Entrypoint CLI
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Extrai dados de um CRI do Google Drive (TS + Aditamentos + Atas), "
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
