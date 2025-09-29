# -*- coding: utf-8 -*-
# ============================
# Imports
# ============================
import os
import tempfile
from datetime import datetime

import pandas as pd
import httplib2

from oauth2client.client import OAuth2Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import openpyxl
from dotenv import load_dotenv

load_dotenv()

# ============================
# Autenticação no Google Drive
# (usa exatamente a função que você forneceu)
# ============================
def _parse_iso8601_or_none(value: str):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def _getenv(*keys, default=None):
    """Retorna o primeiro valor não vazio dentre as chaves."""
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    return default

def conectar_drive():
    """
    Conecta ao Google Drive lendo variáveis com e sem prefixo GDRIVE_.
    Obrigatórios: CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN (aceita GDRIVE_*).
    Opcionais: ACCESS_TOKEN, TOKEN_EXPIRY, TOKEN_URI, REVOKE_URI (aceita GDRIVE_*).
    """
    client_id     = _getenv("GDRIVE_CLIENT_ID", "CLIENT_ID")
    client_secret = _getenv("GDRIVE_CLIENT_SECRET", "CLIENT_SECRET")
    refresh_token = _getenv("GDRIVE_REFRESH_TOKEN", "REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        faltando = []
        if not client_id:     faltando.append("GDRIVE_CLIENT_ID/CLIENT_ID")
        if not client_secret: faltando.append("GDRIVE_CLIENT_SECRET/CLIENT_SECRET")
        if not refresh_token: faltando.append("GDRIVE_REFRESH_TOKEN/REFRESH_TOKEN")
        raise RuntimeError("Variáveis obrigatórias ausentes: " + ", ".join(faltando))

    token_uri   = _getenv("GDRIVE_TOKEN_URI", "TOKEN_URI", default="https://oauth2.googleapis.com/token")
    revoke_uri  = _getenv("GDRIVE_REVOKE_URI", "REVOKE_URI")  # opcional
    access_tok  = _getenv("GDRIVE_ACCESS_TOKEN", "ACCESS_TOKEN")  # opcional
    token_exp   = _parse_iso8601_or_none(_getenv("GDRIVE_TOKEN_EXPIRY", "TOKEN_EXPIRY"))

    credentials = OAuth2Credentials(
        access_token=access_tok,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        token_expiry=token_exp,       # pode ser None
        token_uri=token_uri,          # com default garantido
        user_agent="streamlit-app/1.0",
        revoke_uri=revoke_uri         # pode ser None
    )

    # força refresh quando não há expiry/token ou se estiver expirado
    try:
        if (token_exp is None) or credentials.access_token_expired or not access_tok:
            credentials.refresh(httplib2.Http())
    except Exception as e:
        raise RuntimeError(
            "Falha ao atualizar o access_token. "
            "Verifique CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN e TOKEN_URI. "
            f"Endpoint TOKEN_URI usado: {token_uri}. Detalhe: {e}"
        )

    gauth = GoogleAuth()
    gauth.credentials = credentials
    return GoogleDrive(gauth)

# ============================
# Utilitários de navegação/leitura
# ============================
def _localizar_pasta(drive, nome_pasta, parent_id=None):
    """Retorna o primeiro folder com título exato `nome_pasta`.
    Se parent_id for None, busca na raiz; caso contrário, busca dentro do parent."""
    if parent_id:
        query = (
            f"'{parent_id}' in parents and title = '{nome_pasta}' "
            "and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
    else:
        query = (
            f"title = '{nome_pasta}' "
            "and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
    pastas = drive.ListFile({'q': query}).GetList()
    return pastas[0] if pastas else None


def _baixar_excel_por_nome(drive, pasta_id, titulo_arquivo):
    """Baixa um arquivo pelo título exato dentro de `pasta_id` e retorna caminho local temporário.
    Faz uma segunda tentativa case-insensitive se necessário."""
    # Busca exata
    query = f"'{pasta_id}' in parents and title = '{titulo_arquivo}' and trashed = false"
    itens = drive.ListFile({'q': query}).GetList()

    # Fallback case-insensitive
    if not itens:
        lista = drive.ListFile({'q': f"'{pasta_id}' in parents and trashed = false"}).GetList()
        alvo_lower = titulo_arquivo.lower()
        itens = [i for i in lista if i.get('title', '').lower() == alvo_lower]

    if not itens:
        raise FileNotFoundError(f"Arquivo não encontrado na pasta: {titulo_arquivo}")

    tmp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx").name
    itens[0].GetContentFile(tmp_path)
    return tmp_path


# ============================
# Carregamento das bases
# ============================
def carregar_bases_adiantamento(
    nomes_arquivos=None,
    sheets=None,
    read_kwargs=None,
):
    """
    Lê os arquivos na pasta Google Drive:
      Adiantamento_APP / bases /
        - De_Para PO 0600x1200.xlsx
        - Base RZE - 200825.XLSX
        - Base PM - 200825.XLSX
        - Base IN - 200825.xlsx

    Retorna:
        df_pm, df_in, df_rze, df_0600 (nesta ordem)

    Parâmetros opcionais:
      - nomes_arquivos: dict para customizar títulos dos arquivos no Drive.
      - sheets: dict com nome da aba por arquivo (ou None para padrão).
      - read_kwargs: dict com parâmetros extras do pandas.read_excel (ex.: converters, dtype etc.).
    """
    # Nomes padrões conforme a imagem
    NOMES_ARQUIVOS_PADRAO = {
        "pm":   "PM Completo - 160925.XLSX",
        "in":   "IN Completo - 160925.xlsx",
        "rze":  "RZE Completo - 160925.XLSX",
        "0600": "De_Para PO 0600x1200.xlsx",
        "expurgo": "Lista de POs expurgadas.xlsx",
        "inpago": "IN pago ou baixado.xlsx",
        "baixas": "Outras baixas.xlsx"
    }
    nomes = nomes_arquivos or NOMES_ARQUIVOS_PADRAO

    # Abas padrão (None => primeira aba)
    SHEETS_PADRAO = {
        "pm":   None,
        "in":   None,
        "rze":  None,
        "0600": None,
        "expurgo": None,
        "inpago": None,
        "baixas": None
    }
    sheets = sheets or SHEETS_PADRAO

    # Parâmetros de leitura padrão
    read_defaults = dict(dtype=str)  # preserva zeros à esquerda e evita conversões indevidas
    if read_kwargs:
        read_defaults.update(read_kwargs)

    # Conecta no Drive
    drive = conectar_drive()

    # Navega: Adiantamento_APP / bases
    pasta_root = _localizar_pasta(drive, "Adiantamento_APP")
    if not pasta_root:
        raise FileNotFoundError("Pasta 'Adiantamento_APP' não encontrada no seu Drive.")

    pasta_bases = _localizar_pasta(drive, "bases", parent_id=pasta_root['id'])
    if not pasta_bases:
        raise FileNotFoundError("Subpasta 'bases' não encontrada dentro de 'Adiantamento_APP'.")

    # Baixa cada arquivo para temp
    p_pm   = _baixar_excel_por_nome(drive, pasta_bases['id'], nomes["pm"])
    p_in   = _baixar_excel_por_nome(drive, pasta_bases['id'], nomes["in"])
    p_rze  = _baixar_excel_por_nome(drive, pasta_bases['id'], nomes["rze"])
    p_0600 = _baixar_excel_por_nome(drive, pasta_bases['id'], nomes["0600"])
    p_expurgo = _baixar_excel_por_nome(drive, pasta_bases['id'], nomes["expurgo"])
    p_inpago = _baixar_excel_por_nome(drive, pasta_bases['id'], nomes["inpago"])
    p_baixas = _baixar_excel_por_nome(drive, pasta_bases['id'], nomes["baixas"])

    # Lê com pandas
    df_pm   = pd.read_excel(p_pm,   sheet_name=sheets["pm"],   **read_defaults)
    df_in   = pd.read_excel(p_in,   sheet_name=sheets["in"],   **read_defaults)
    df_rze  = pd.read_excel(p_rze,  sheet_name=sheets["rze"],  **read_defaults)
    df_0600 = pd.read_excel(p_0600, sheet_name=sheets["0600"], **read_defaults)
    df_expurgo = pd.read_excel(p_expurgo, sheet_name=sheets["expurgo"], **read_defaults)
    df_inpago = pd.read_excel(p_inpago, sheet_name=sheets["inpago"], **read_defaults)
    df_baixas = pd.read_excel(p_baixas, sheet_name=sheets["baixas"], **read_defaults)

    return df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_baixas


# ============================
# Exemplo de uso direto
# (remova/adicione prints conforme necessário)
# ============================
if __name__ == "__main__":
    df_pm, df_in, df_rze, df_0600 = carregar_bases_adiantamento()

    print("Bases carregadas com sucesso:")
    for nome, df in [
        ("df_pm", df_pm),
        ("df_in", df_in),
        ("df_rze", df_rze),
        ("df_0600", df_0600),
    ]:
        try:
            print(f" - {nome}: {df.shape[0]} linhas x {df.shape[1]} colunas")
        except Exception:
            # caso a planilha venha como dict de DataFrames (múltiplas abas)
            print(f" - {nome}: múltiplas abas (keys: {list(df.keys())})")
