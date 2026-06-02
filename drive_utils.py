"""
Utilitários para autenticação e download de arquivos do Google Drive.

Autenticação exclusivamente via Service Account (GOOGLE_SERVICE_ACCOUNT_JSON).
Projetado para estabilidade em ambientes de microsserviços.
"""

import io
import json
import logging
import os
import unicodedata
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

load_dotenv()

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# Palavras-chave para priorizar o PDF correto quando há múltiplos na pasta
_PALAVRAS_CHAVE_TERMO = ("termo", "securitizacao", "securitizacao")


def _normalizar(texto: str) -> str:
    """Remove acentos e converte para minúsculas para comparação robusta."""
    nfkd = unicodedata.normalize("NFKD", texto)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return sem_acento.lower()


def obter_servico():
    """
    Cria e retorna um cliente autenticado da API do Google Drive.

    Lê o JSON completo da credencial a partir da variável de ambiente
    GOOGLE_SERVICE_ACCOUNT_JSON. Nunca depende de arquivos em disco.

    Returns:
        Resource: cliente autenticado do Google Drive v3.

    Raises:
        RuntimeError: se a variável de ambiente estiver ausente ou inválida.
    """
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError(
            "Variável de ambiente GOOGLE_SERVICE_ACCOUNT_JSON não definida. "
            "Verifique seu arquivo .env."
        )

    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON contém JSON inválido: {exc}"
        ) from exc

    credentials = service_account.Credentials.from_service_account_info(
        info, scopes=_SCOPES
    )
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)
    logger.debug("Serviço Google Drive autenticado via Service Account.")
    return service


# ---------------------------------------------------------------------------
# Busca
# ---------------------------------------------------------------------------


def _buscar_subpasta(service, pasta_raiz_id: str, nome_exato: str) -> str | None:
    """Retorna o ID da primeira subpasta com nome exatamente igual a nome_exato."""
    query = (
        f"'{pasta_raiz_id}' in parents"
        f" and mimeType = 'application/vnd.google-apps.folder'"
        f" and name = '{nome_exato}'"
        f" and trashed = false"
    )
    try:
        resposta = (
            service.files()
            .list(q=query, fields="files(id, name)", pageSize=5)
            .execute()
        )
    except HttpError as exc:
        logger.error(
            "Erro ao buscar subpasta '%s' em '%s': %s", nome_exato, pasta_raiz_id, exc
        )
        return None

    arquivos = resposta.get("files", [])
    if not arquivos:
        logger.warning("Subpasta '%s' não encontrada em '%s'.", nome_exato, pasta_raiz_id)
        return None

    if len(arquivos) > 1:
        logger.warning(
            "%d subpastas com o nome '%s' encontradas; usando a primeira.",
            len(arquivos),
            nome_exato,
        )

    return arquivos[0]["id"]


def _listar_pdfs(service, pasta_id: str) -> list[dict]:
    """Lista todos os PDFs (não na lixeira) dentro de pasta_id."""
    query = (
        f"'{pasta_id}' in parents"
        f" and mimeType = 'application/pdf'"
        f" and trashed = false"
    )
    pdfs: list[dict] = []
    page_token: str | None = None

    while True:
        try:
            params: dict = {
                "q": query,
                "fields": "nextPageToken, files(id, name)",
                "pageSize": 100,
            }
            if page_token:
                params["pageToken"] = page_token

            resposta = service.files().list(**params).execute()
        except HttpError as exc:
            logger.error("Erro ao listar PDFs em '%s': %s", pasta_id, exc)
            break

        pdfs.extend(resposta.get("files", []))
        page_token = resposta.get("nextPageToken")
        if not page_token:
            break

    return pdfs


def encontrar_termo_por_codigo_if(service, codigo_if: str) -> str | None:
    """
    Localiza o ID do PDF do Termo de Securitização de um CRI no Google Drive.

    Fluxo:
      1. Lê DRIVE_FOLDER_CRIS_ID do ambiente.
      2. Busca subpasta com nome == codigo_if.
      3. Lista os PDFs nessa subpasta.
      4. Se houver apenas um, retorna seu ID.
      5. Se houver vários, prioriza o que contiver "termo", "securitizacao"
         ou "securitização" no nome (case-insensitive, sem acento).
      6. Retorna None se nada for encontrado.

    Args:
        service: cliente autenticado retornado por obter_servico().
        codigo_if: código IF do CRI (nome exato da subpasta no Drive).

    Returns:
        ID do arquivo PDF ou None.
    """
    pasta_raiz_id = os.environ.get("DRIVE_FOLDER_CRIS_ID")
    if not pasta_raiz_id:
        raise RuntimeError(
            "Variável de ambiente DRIVE_FOLDER_CRIS_ID não definida. "
            "Verifique seu arquivo .env."
        )

    logger.info("Buscando Termo de Securitização para código IF '%s'...", codigo_if)

    subpasta_id = _buscar_subpasta(service, pasta_raiz_id, codigo_if)
    if not subpasta_id:
        return None

    pdfs = _listar_pdfs(service, subpasta_id)
    if not pdfs:
        logger.warning("Nenhum PDF encontrado na pasta do CRI '%s'.", codigo_if)
        return None

    if len(pdfs) == 1:
        logger.info("Um único PDF encontrado: '%s'.", pdfs[0]["name"])
        return pdfs[0]["id"]

    # Múltiplos PDFs: prioriza pelo nome
    logger.info(
        "%d PDFs encontrados para '%s'. Tentando identificar o Termo de Securitização.",
        len(pdfs),
        codigo_if,
    )
    for pdf in pdfs:
        nome_normalizado = _normalizar(pdf["name"])
        if any(kw in nome_normalizado for kw in _PALAVRAS_CHAVE_TERMO):
            logger.info("Termo de Securitização identificado: '%s'.", pdf["name"])
            return pdf["id"]

    # Nenhum nome coincide: retorna o primeiro e avisa
    logger.warning(
        "Múltiplos PDFs encontrados para '%s' mas nenhum contém palavras-chave "
        "esperadas. Retornando o primeiro: '%s'.",
        codigo_if,
        pdfs[0]["name"],
    )
    return pdfs[0]["id"]


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def baixar_pdf_drive(service, file_id: str, diretorio_destino: str) -> str:
    """
    Faz o download de um PDF do Google Drive em blocos (chunked) e salva localmente.

    Args:
        service: cliente autenticado retornado por obter_servico().
        file_id: ID do arquivo no Google Drive.
        diretorio_destino: caminho do diretório onde o arquivo será salvo.

    Returns:
        Caminho absoluto do arquivo baixado.

    Raises:
        HttpError: em caso de erro de API não recuperável.
        OSError: em caso de falha de escrita no disco.
    """
    destino = Path(diretorio_destino)
    destino.mkdir(parents=True, exist_ok=True)

    # Obtém metadados para usar o nome original do arquivo
    try:
        meta = service.files().get(fileId=file_id, fields="name, size").execute()
    except HttpError as exc:
        logger.error("Erro ao obter metadados do arquivo '%s': %s", file_id, exc)
        raise

    nome_arquivo = meta.get("name", f"{file_id}.pdf")
    tamanho = int(meta.get("size", 0))
    caminho_final = destino / nome_arquivo

    logger.info(
        "Iniciando download: '%s' (%.1f MB) → '%s'",
        nome_arquivo,
        tamanho / 1_048_576,
        caminho_final,
    )

    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request, chunksize=8 * 1024 * 1024)  # 8 MB

    concluido = False
    while not concluido:
        try:
            status, concluido = downloader.next_chunk()
        except HttpError as exc:
            logger.error("Falha durante o download de '%s': %s", nome_arquivo, exc)
            raise

        if status:
            logger.debug(
                "Download de '%s': %.0f%%", nome_arquivo, status.progress() * 100
            )

    caminho_final.write_bytes(buffer.getvalue())
    logger.info("Arquivo salvo em '%s'.", caminho_final)
    return str(caminho_final)
