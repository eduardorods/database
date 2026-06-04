import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import select

from database import SessionLocal
from models import CRIClausula, CRIEvento, CRIInformeMensal, CRIMetadata, CRISerie

load_dotenv()

st.set_page_config(page_title="Monitor de CRIs", layout="wide")

st.markdown("""
<style>
/* ── Fonte e espaçamento global ── */
html, body, [class*="css"], .stApp {
    font-size: 13px !important;
    line-height: 1.4 !important;
    font-family: "Inter", "Segoe UI", system-ui, sans-serif !important;
}

/* ── Padding da área principal ── */
.block-container {
    padding-top: 1rem !important;
    padding-bottom: 1rem !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
    max-width: 100% !important;
}

/* ── Cabeçalhos ── */
h1 { font-size: 1.25rem !important; margin: 0 0 0.1rem 0 !important; font-weight: 600 !important; }
h2 { font-size: 1.05rem !important; margin: 0.5rem 0 0.25rem 0 !important; font-weight: 600 !important; }
h3 { font-size: 0.95rem !important; margin: 0.4rem 0 0.2rem 0 !important; font-weight: 600 !important; }

/* ── Métricas (st.metric) ── */
[data-testid="stMetricLabel"] {
    font-size: 11px !important;
    color: #6b7280 !important;
    font-weight: 500 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.03em !important;
    margin-bottom: 0 !important;
}
[data-testid="stMetricValue"] {
    font-size: 13px !important;
    font-weight: 600 !important;
    line-height: 1.3 !important;
}
[data-testid="metric-container"] {
    background: #f8f9fb !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 6px !important;
    padding: 8px 12px !important;
}

/* ── Tabelas Markdown (termos e cronograma) ── */
table {
    border-collapse: collapse !important;
    width: 100% !important;
    font-size: 10px !important;
    margin: 0.5rem 0 !important;
}
th {
    background-color: #f1f3f5 !important;
    font-size: 10px !important;
    font-weight: 600 !important;
    padding: 4px 8px !important;
    border: 1px solid #d1d5db !important;
    text-align: left !important;
    white-space: nowrap !important;
}
td {
    font-size: 10px !important;
    padding: 3px 8px !important;
    border: 1px solid #e5e7eb !important;
    vertical-align: top !important;
}
tr:nth-child(even) td { background-color: #f9fafb !important; }

/* ── Dataframe (séries) ── */
[data-testid="stDataFrame"] iframe { min-height: unset !important; }
.dvn-scroller { font-size: 12px !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"] { font-size: 13px !important; }
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 { font-size: 0.9rem !important; }

/* ── Divider ── */
hr { margin: 0.5rem 0 !important; border-color: #e5e7eb !important; }

/* ── Caption / texto auxiliar ── */
[data-testid="stCaptionContainer"], small, caption {
    font-size: 11px !important;
    color: #6b7280 !important;
}

/* ── Texto corrido nas seções ── */
[data-testid="stMarkdownContainer"] p {
    font-size: 13px !important;
    line-height: 1.55 !important;
    margin-bottom: 0.4rem !important;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Consultas ao banco
# ---------------------------------------------------------------------------


def _buscar_cri(codigo_if: str) -> CRIMetadata | None:
    with SessionLocal() as session:
        resultado = session.execute(
            select(CRIMetadata)
            .where(CRIMetadata.codigo_if == codigo_if)
            .order_by(CRIMetadata.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if resultado:
            session.expunge(resultado)
        return resultado


def _buscar_series(cri_id) -> list[dict]:
    with SessionLocal() as session:
        rows = session.execute(
            select(CRISerie).where(CRISerie.cri_id == cri_id)
        ).scalars().all()
        return [
            {
                "Série": r.nome_serie or "—",
                "Vencimento": r.data_vencimento.strftime("%d/%m/%Y") if r.data_vencimento else "—",
                "Indexador": r.indexador or "—",
                "Taxa / Spread (% a.a.)": float(r.taxa_spread) if r.taxa_spread else "—",
            }
            for r in rows
        ]


def _buscar_clausula(cri_id, tipo: str) -> str:
    with SessionLocal() as session:
        row = session.execute(
            select(CRIClausula).where(
                CRIClausula.cri_id == cri_id,
                CRIClausula.tipo_clausula == tipo,
            )
        ).scalar_one_or_none()
        return row.texto_original or "" if row else ""


def _buscar_eventos(cri_id, tipo: str) -> list:
    with SessionLocal() as session:
        rows = session.execute(
            select(CRIEvento)
            .where(CRIEvento.cri_id == cri_id)
            .order_by(CRIEvento.data_evento)
        ).scalars().all()
        filtrados = [
            r for r in rows
            if r.tipo_documento and tipo.lower() in r.tipo_documento.lower()
        ]
        for r in filtrados:
            session.expunge(r)
        return filtrados


def _buscar_informes(cri_id) -> list[dict]:
    with SessionLocal() as session:
        rows = session.execute(
            select(CRIInformeMensal)
            .where(CRIInformeMensal.cri_id == cri_id)
            .order_by(CRIInformeMensal.mes_referencia.desc())
        ).scalars().all()
        return [
            {
                "mes_referencia": r.mes_referencia,
                "serie": r.serie,
                "saldo_devedor": r.saldo_devedor,
                "valor_integralizado": r.valor_integralizado,
                "indexador_atual": r.indexador_atual,
                "spread_atual": r.spread_atual,
            }
            for r in rows
        ]


def _fmt_brl(v: float | None) -> str:
    """Formata float para Real brasileiro. Retorna 'Não informado' para None ou zero."""
    if v is None or v == 0.0:
        return "Não informado"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_pct(v: float | None) -> str:
    """Formata float como percentual. Retorna 'Não informado' para None ou zero."""
    if v is None or v == 0.0:
        return "Não informado"
    return f"{v:.4f}%".rstrip("0").rstrip(".")


def _render_markdown(texto: str) -> None:
    """Renderiza texto do banco garantindo que \\n escapados virem quebras reais."""
    st.markdown(texto.replace("\\n", "\n"), unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Componentes de UI
# ---------------------------------------------------------------------------


def _render_header(cri: CRIMetadata) -> None:
    emissao = f"{cri.numero_emissao}ª Emissão" if cri.numero_emissao else "Emissão —"
    securitizadora = cri.securitizadora or "Securitizadora não informada"
    st.title(f"{emissao} · {securitizadora}")
    st.caption(f"Código IF: `{cri.codigo_if}`")

    partes = []
    if cri.lastro_operacao:
        partes.append(f"**Lastro:** {cri.lastro_operacao}")
    if cri.cedente:
        partes.append(f"**Cedente:** {cri.cedente}")
    if cri.devedor_principal:
        partes.append(f"**Devedor Principal:** {cri.devedor_principal}")
    if partes:
        st.caption("  ·  ".join(partes))

    st.divider()


def _render_metricas(cri: CRIMetadata) -> None:
    st.header("Metadados")
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)

    col1.metric("Devedor", cri.devedor or "—")
    col2.metric("Agente Fiduciário", cri.agente_fiduciario or "—")
    col3.metric(
        "Data de Emissão",
        cri.data_emissao.strftime("%d/%m/%Y") if cri.data_emissao else "—",
    )
    col4.metric("Frequência de Juros", cri.frequencia_juros or "—")
    col5.metric(
        "Data Início Juros",
        cri.data_inicio_juros.strftime("%d/%m/%Y") if cri.data_inicio_juros else "—",
    )
    col6.metric("Frequência de Amortização", cri.frequencia_amortizacao or "—")
    st.divider()


def _render_series(cri_id) -> None:
    st.header("Séries")
    series_ts = _buscar_series(cri_id)

    if not series_ts:
        st.info("Nenhuma série encontrada para este CRI.")
        st.divider()
        return

    st.caption("**Dados de Emissão (Termo de Securitização)**")
    st.dataframe(series_ts, use_container_width=True, hide_index=True)
    st.divider()


def _render_informe_mensal(cri_id) -> None:
    st.header("Informe Mensal Mais Recente")
    informes = _buscar_informes(cri_id)

    if not informes:
        st.caption("Informe Mensal não processado para este CRI.")
        st.divider()
        return

    mes_ref = informes[0]["mes_referencia"] or "—"
    st.info(f"Dados extraídos do Informe Mensal de: **{mes_ref}**")

    # Mantém apenas o registro mais recente por série
    informe_idx: dict[str, dict] = {}
    for inf in informes:
        k = (inf["serie"] or "").strip().lower()
        if k not in informe_idx:
            informe_idx[k] = inf

    rows = []
    for inf in informe_idx.values():
        rows.append({
            "Série": inf["serie"] or "—",
            "Saldo Devedor": _fmt_brl(inf["saldo_devedor"]),
            "Valor Integralizado": _fmt_brl(inf["valor_integralizado"]),
            "Indexador": inf["indexador_atual"] or "—",
            "Spread (% a.a.)": _fmt_pct(inf["spread_atual"]),
        })

    st.dataframe(rows, use_container_width=True, hide_index=True)
    st.divider()


def _render_termos_definidos(cri_id) -> None:
    st.header("Termos Definidos")
    texto = _buscar_clausula(cri_id, "termos_definidos")
    if texto.strip():
        _render_markdown(texto)
    else:
        st.caption("Termos definidos não extraídos para este CRI.")
    st.divider()


def _render_cronogramas(cri_id) -> None:
    st.header("Cronogramas de Pagamento")
    texto = _buscar_clausula(cri_id, "cronograma_pagamentos")
    if texto.strip():
        _render_markdown(texto)
    else:
        st.caption("Cronograma não extraído para este CRI.")
    st.divider()


def _render_historico_eventos(cri_id) -> None:
    st.header("Histórico de Eventos")
    for tipo, titulo in [("ADITAMENTO", "Aditamentos"), ("ATA", "Atas de Assembleia")]:
        st.subheader(titulo)
        eventos = _buscar_eventos(cri_id, tipo)
        if not eventos:
            st.caption(f"Nenhum(a) {titulo.lower()} encontrado(a) para este CRI.")
        else:
            for evento in eventos:
                label = f"{evento.data_evento or 'Data não identificada'} — {evento.nome_arquivo or ''}"
                with st.expander(label):
                    if evento.resumo:
                        st.markdown(evento.resumo.replace("\\n", "\n"), unsafe_allow_html=True)
                    else:
                        st.caption("Resumo não disponível.")


# ---------------------------------------------------------------------------
# App principal
# ---------------------------------------------------------------------------


def main() -> None:
    with st.sidebar:
        st.header("Buscar CRI")
        codigo_if = st.text_input(
            "Código IF",
            placeholder="Ex: 19K1139273",
        ).strip()
        buscar = st.button("Buscar CRI", type="primary", use_container_width=True)

    if not buscar:
        st.info("Digite um Código IF na barra lateral e clique em **Buscar CRI**.")
        return

    if not codigo_if:
        st.warning("Por favor, informe o Código IF antes de buscar.")
        return

    with st.spinner(f"Consultando dados para `{codigo_if}`..."):
        cri = _buscar_cri(codigo_if)

    if cri is None:
        st.warning(
            f"Nenhum CRI encontrado para o código IF **{codigo_if}**. "
            "Verifique o código ou execute o pipeline de extração primeiro."
        )
        return

    _render_header(cri)                # título, securitizadora, lastro/cedente/devedor
    _render_metricas(cri)              # Seção 1 — Metadados
    _render_series(cri.id)             # Seção 2 — Séries (Emissão vs. Informe Mensal)
    _render_informe_mensal(cri.id)     # Seção 3 — Informe Mensal Mais Recente
    _render_termos_definidos(cri.id)   # Seção 4 — Termos Definidos
    _render_cronogramas(cri.id)        # Seção 5 — Cronogramas
    _render_historico_eventos(cri.id)  # Seção 6 — Histórico de Eventos


if __name__ == "__main__":
    main()
