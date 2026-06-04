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
from drive_utils import (
    _buscar_subpasta,
    _listar_pdfs,
    _normalizar,
    baixar_documentos_cri,
    baixar_pdf_drive,
    obter_servico,
)
from models import CRIClausula, CRIEvento, CRIInformeMensal, CRIMetadata, CRISerie

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

   ⚠️ ATENÇÃO CRÍTICA — ESTRUTURA TABULAR:
   Os Termos Definidos frequentemente NÃO estão em texto corrido. Eles podem estar formatados
   como uma tabela de duas colunas no documento, onde a primeira coluna contém o TERMO (ex:
   "Agente Fiduciário:", "Instituição Custodiante:") e a segunda coluna contém a DESCRIÇÃO.
   VOCÊ DEVE identificar essa estrutura tabular e convertê-la para a tabela Markdown exigida,
   mapeando cada linha da tabela original como uma linha | Termo | Descrição | na saída.

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
7. CAMPO "devedor_principal": Busque nos Termos Definidos pela entrada cujo termo seja
   "Devedor", "Devedora" ou "Companhia" para identificar o nome da entidade devedora.
   O gênero da palavra (Devedor/Devedora) não importa — extraia o nome da empresa ou fundo.
   Se não encontrar nos Termos Definidos, busque na cláusula principal da operação.
8. CAMPO "indexador" em cada série: Retorne EXATAMENTE APENAS a sigla do índice
   (ex: "IPCA", "CDI", "TR", "IGPM", "IPCA+", "prefixado").
   É ESTRITAMENTE PROIBIDO retornar expressões compostas como "IPCA + spread",
   "CDI + 2,5%" ou qualquer combinação neste campo. O spread percentual vai
   EXCLUSIVAMENTE no campo "taxa_spread".

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
    "frequencia_amortizacao": "",
    "lastro_operacao": "",
    "cedente": "",
    "devedor_principal": ""
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

# Sprint 5: extração de dados financeiros do Informe Mensal
PROMPT_INFORME_TEMPLATE = """\
O documento em anexo é um Informe Mensal de CRI (Certificado de Recebíveis Imobiliários).

Leia o documento completo e extraia as informações financeiras de CADA série presente.

🚨 REGRA PARA NÚMEROS — VIOLAÇÃO INUTILIZA O RESULTADO:
Para os campos saldo_devedor, valor_integralizado e spread_atual você DEVE retornar
EXATAMENTE um número float no formato de programação (ex: 1500000.50).
- É ESTRITAMENTE PROIBIDO usar o símbolo R$, pontos de milhar ou vírgulas decimais.
- Converta o formato brasileiro para float: "R$ 1.500.000,50" → 1500000.50
- "2,5% a.a." → 2.5
- Se o dado não existir no documento, retorne 0.0 (nunca null para esses campos).

Retorne APENAS o JSON abaixo, sem ```json ao redor. Qualquer quebra de linha deve ser \\n.
mes_referencia no formato "MM/AAAA". indexador_atual: apenas a sigla (ex: "IPCA", "CDI").

{{
  "mes_referencia": "MM/AAAA",
  "series": [
    {{
      "serie": "nome ou número da série (ex: 1ª Série, Série Sênior)",
      "saldo_devedor": 0.0,
      "valor_integralizado": 0.0,
      "indexador_atual": "IPCA",
      "spread_atual": 0.0
    }}
  ]
}}
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
# Passo Zero — Placeholder de integração com scraper externo
# ---------------------------------------------------------------------------


def _acionar_scraper_fundos_net(codigo_if: str) -> None:
    """
    [PASSO ZERO — PLACEHOLDER] Aciona o scraper do fundos.net para o código IF informado.

    Em produção, esta função deve:
      1. Baixar o Termo de Securitização mais recente.
      2. Baixar Aditamentos disponíveis.
      3. Baixar Atas de Assembleia disponíveis.
      4. Baixar o Informe Mensal mais recente.
      5. Salvar todos os arquivos na pasta correspondente do Google Drive.
    """
    logger.info("━" * 64)
    logger.info("[PASSO ZERO] Acionando scraper fundos.net para '%s'...", codigo_if)
    logger.info("  → [TODO] Baixar Termo de Securitização mais recente...")
    logger.info("  → [TODO] Baixar Aditamentos disponíveis...")
    logger.info("  → [TODO] Baixar Atas de Assembleia disponíveis...")
    logger.info("  → [TODO] Baixar Informe Mensal mais recente...")
    logger.info("  ⚠️  Scraper não implementado. Usando arquivos já presentes no Drive.")
    logger.info("━" * 64)


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
# Human-in-the-Loop — Validação interativa no terminal
# ---------------------------------------------------------------------------


def _validar_dados_human_in_the_loop(dados: dict) -> dict:
    """
    Verifica anomalias no JSON extraído e solicita correção humana via terminal.

    Regras:
      1. Lastro não identificado → solicita digitação.
      2. Spread de série < 1.0 ou ausente → pausa e confirma (possível erro IPCA).
      3. Campos críticos de metadata ausentes → permite inserção manual.
    """
    meta = dados.setdefault("metadata", {})
    series = dados.get("series", [])

    print()  # linha em branco para separar do log

    # ── Checagem 1: lastro_operacao ────────────────────────────────────────
    if not meta.get("lastro_operacao"):
        valor = input(
            "⚠️  Lastro não identificado. "
            "Digite o lastro da operação (ex: Debêntures, CCB, CCV): "
        ).strip()
        if valor:
            meta["lastro_operacao"] = valor
            logger.info("Lastro definido manualmente: '%s'.", valor)

    # ── Checagem 2: spread por série ───────────────────────────────────────
    for i, serie in enumerate(series, start=1):
        spread = serie.get("taxa_spread")
        nome = serie.get("nome_serie") or f"Série {i}"
        spread_num = None
        try:
            spread_num = float(spread) if spread is not None else None
        except (TypeError, ValueError):
            pass

        if spread_num is None or spread_num == 0.0 or spread_num < 1.0:
            resposta = input(
                f"⚠️  Anomalia detectada: Spread da {nome} é {spread}% "
                f"(possível erro para IPCA+). Confirma este valor? (s/n): "
            ).strip().lower()
            if resposta == "n":
                novo = input(
                    f"   Digite o spread correto para {nome} (ex: 2.5): "
                ).strip()
                try:
                    serie["taxa_spread"] = float(novo.replace(",", "."))
                    logger.info(
                        "Spread da %s corrigido manualmente: %s%%.", nome, novo
                    )
                except ValueError:
                    logger.warning(
                        "Valor '%s' inválido para spread. Mantendo original.", novo
                    )

    # ── Checagem 3: campos críticos ausentes ───────────────────────────────
    campos_criticos = ["securitizadora", "devedor", "numero_emissao"]
    faltando = [c for c in campos_criticos if not meta.get(c)]
    if faltando:
        print(
            f"⚠️  Extração falhou em campos críticos: {faltando}. "
            "Insira manualmente ou pressione Enter para pular."
        )
        for campo in faltando:
            valor = input(f"   {campo}: ").strip()
            if valor:
                meta[campo] = int(valor) if campo == "numero_emissao" and valor.isdigit() else valor
                logger.info("Campo '%s' preenchido manualmente: '%s'.", campo, valor)

    print()  # linha em branco ao finalizar
    return dados


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
            lastro_operacao=meta.get("lastro_operacao") or None,
            cedente=meta.get("cedente") or None,
            devedor_principal=meta.get("devedor_principal") or None,
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
# Sprint 5 — Informe Mensal
# ---------------------------------------------------------------------------


def _baixar_informe_mensal(service, codigo_if: str, diretorio_destino: str) -> str | None:
    """Localiza e baixa o PDF do Informe Mensal mais recente da pasta do CRI no Drive."""
    pasta_raiz_id = os.environ.get("DRIVE_FOLDER_CRIS_ID")
    if not pasta_raiz_id:
        raise RuntimeError("Variável de ambiente DRIVE_FOLDER_CRIS_ID não definida.")

    subpasta_id = _buscar_subpasta(service, pasta_raiz_id, codigo_if)
    if not subpasta_id:
        return None

    pdfs = _listar_pdfs(service, subpasta_id)
    informes = [p for p in pdfs if "informe_mensal" in _normalizar(p["name"])]

    if not informes:
        logger.info("Nenhum Informe Mensal encontrado na pasta '%s'.", codigo_if)
        return None

    # Mais recente = maior nome lexicográfico (assumindo padrão com data no nome)
    informe = sorted(informes, key=lambda p: p["name"])[-1]
    logger.info("Informe Mensal identificado: '%s'.", informe["name"])

    try:
        return baixar_pdf_drive(service, informe["id"], diretorio_destino)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Erro ao baixar Informe Mensal: %s", exc)
        return None


def _extrair_informe_mensal_gemini(caminho_pdf: str) -> list[dict]:
    """Extrai dados financeiros de cada série do Informe Mensal via Gemini."""
    logger.info("Extraindo Informe Mensal: '%s'...", Path(caminho_pdf).name)
    pdf_bytes = Path(caminho_pdf).read_bytes()

    model = genai.GenerativeModel("gemini-2.5-flash-lite")
    resposta = model.generate_content(
        [{"mime_type": "application/pdf", "data": pdf_bytes}, PROMPT_INFORME_TEMPLATE],
        generation_config=genai.GenerationConfig(
            response_mime_type="application/json",
            temperature=0.0,
        ),
    )

    texto_limpo = _sanitizar_json(resposta.text or "")
    try:
        dados = json.loads(texto_limpo, strict=False)
    except json.JSONDecodeError as exc:
        logger.warning("Informe Mensal: JSON inválido (%s). Retornando vazio.", exc)
        return []

    mes_ref = dados.get("mes_referencia")
    series = dados.get("series", [])
    for s in series:
        s["mes_referencia"] = mes_ref

    logger.info("Informe extraído: mês=%s, %d série(s).", mes_ref, len(series))
    return series


def _persistir_informes(cri_id: uuid.UUID, series: list[dict]) -> None:
    """Salva registros de CRIInformeMensal para cada série do informe."""
    if not series:
        return

    def _to_float(v) -> float | None:
        try:
            return float(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    with SessionLocal() as session:
        for s in series:
            session.add(CRIInformeMensal(
                cri_id=cri_id,
                mes_referencia=s.get("mes_referencia"),
                serie=s.get("serie"),
                saldo_devedor=_to_float(s.get("saldo_devedor")),
                valor_integralizado=_to_float(s.get("valor_integralizado")),
                indexador_atual=s.get("indexador_atual"),
                spread_atual=_to_float(s.get("spread_atual")),
            ))
        session.commit()
    logger.info("CRIInformeMensal persistido: %d série(s).", len(series))


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
    caminho_informe: str | None = None

    try:
        # ── Passo Zero: Scraper fundos.net (placeholder) ─────────────────────
        _acionar_scraper_fundos_net(codigo_if)

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

        # ── Human-in-the-Loop: validação e correção interativa ───────────────
        logger.info("Iniciando validação Human-in-the-Loop...")
        dados_ts = _validar_dados_human_in_the_loop(dados_ts)

        cri_id = _persistir_dados(codigo_if, dados_ts)

        # ── Etapa 3: Extração de Eventos ────────────────────────────────────
        total_eventos = len(documentos["aditamentos"]) + len(documentos["atas"])
        logger.info("[3/4] Processando %d evento(s) (Aditamentos + Atas)...", total_eventos)

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

        # ── Etapa 4: Informe Mensal ──────────────────────────────────────────
        logger.info("[4/4] Buscando Informe Mensal...")
        caminho_informe = _baixar_informe_mensal(drive_service, codigo_if, str(TEMP_DIR))
        if caminho_informe:
            try:
                series_informe = _extrair_informe_mensal_gemini(caminho_informe)
                _persistir_informes(cri_id, series_informe)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Falha ao processar Informe Mensal: %s", exc)
        else:
            logger.info("[4/4] Informe Mensal não disponível para este CRI.")

        logger.info("╚══ Pipeline concluído com sucesso para '%s'. ══╝", codigo_if)

    except Exception as exc:
        logger.exception("Erro irrecuperável no pipeline para '%s': %s", codigo_if, exc)
        raise

    finally:
        # Remove todos os PDFs temporários baixados
        todos_caminhos = [documentos["termo_principal"]] if documentos["termo_principal"] else []
        todos_caminhos += documentos.get("aditamentos", [])
        todos_caminhos += documentos.get("atas", [])
        if caminho_informe:
            todos_caminhos.append(caminho_informe)
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
