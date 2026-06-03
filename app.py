import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import select

from database import SessionLocal
from models import CRIClausula, CRIMetadata, CRISerie

load_dotenv()

st.set_page_config(page_title="Monitor de CRIs", layout="wide")

# Mapeamento entre chave interna e rótulo da aba
CLAUSULAS_LABELS = {
    "fundo_reserva": "Fundo de Reserva",
    "fundo_despesa": "Fundo de Despesa",
    "covenants": "Covenants",
    "garantias": "Garantias",
    "amortizacao": "Amortização",
    "cronograma_pagamentos": "Cronograma",
    "termos_definidos": "Termos Definidos",
}


# ---------------------------------------------------------------------------
# Consultas ao banco
# ---------------------------------------------------------------------------


def _buscar_cri(codigo_if: str) -> CRIMetadata | None:
    with SessionLocal() as session:
        resultado = session.execute(
            select(CRIMetadata).where(CRIMetadata.codigo_if == codigo_if)
        ).scalar_one_or_none()
        if resultado:
            # expunge para usar fora da sessão
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


def _buscar_clausulas(cri_id) -> dict[str, str]:
    with SessionLocal() as session:
        rows = session.execute(
            select(CRIClausula).where(CRIClausula.cri_id == cri_id)
        ).scalars().all()
        return {r.tipo_clausula: r.texto_original or "" for r in rows}


# ---------------------------------------------------------------------------
# Componentes de UI
# ---------------------------------------------------------------------------


def _render_header(cri: CRIMetadata) -> None:
    emissao = f"{cri.numero_emissao}ª Emissão" if cri.numero_emissao else "Emissão —"
    securitizadora = cri.securitizadora or "Securitizadora não informada"
    st.title(f"{emissao} · {securitizadora}")
    st.caption(f"Código IF: `{cri.codigo_if}`")
    st.divider()


def _render_metricas(cri: CRIMetadata) -> None:
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
    st.subheader("Séries")
    series = _buscar_series(cri_id)
    if series:
        st.dataframe(series, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma série encontrada para este CRI.")
    st.divider()


def _render_clausulas(cri_id) -> None:
    st.subheader("Cláusulas")
    clausulas = _buscar_clausulas(cri_id)

    if not clausulas:
        st.info("Nenhuma cláusula encontrada para este CRI.")
        return

    abas = st.tabs(list(CLAUSULAS_LABELS.values()))
    for aba, chave in zip(abas, CLAUSULAS_LABELS.keys()):
        with aba:
            texto = clausulas.get(chave, "")
            if texto.strip():
                st.markdown(texto)
            else:
                st.caption("Cláusula não encontrada no documento.")


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

    _render_header(cri)
    _render_metricas(cri)
    _render_series(cri.id)
    _render_clausulas(cri.id)


if __name__ == "__main__":
    main()
