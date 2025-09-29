# -*- coding: utf-8 -*-
# ============================================================
# Salvar DataFrame em CSV no Google Drive (Adiantamentos_APP/bases_app)
# Requer: conectar_drive(), _localizar_pasta() do seu código-base
# Dependências: pandas, PyDrive
# ============================================================
import os
import tempfile
from datetime import datetime
import pandas as pd
from dados.ingestao import _localizar_pasta, conectar_drive
import time

# ------------------------------------------------------------
# Utilitários de pasta
# ------------------------------------------------------------
def _criar_pasta(drive, nome_pasta, parent_id=None):
    meta = {
        'title': nome_pasta,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        meta['parents'] = [{'id': parent_id}]
    folder = drive.CreateFile(meta)
    folder.Upload()
    return folder

def _localizar_ou_criar_pasta(drive, nome_pasta, parent_id=None):
    pasta = _localizar_pasta(drive, nome_pasta, parent_id=parent_id)
    if pasta:
        return pasta
    return _criar_pasta(drive, nome_pasta, parent_id)

def _garantir_caminho_pastas(drive, caminho_pastas):
    """
    caminho_pastas: lista como ["Adiantamento_APP", "bases_app"]
    Retorna o ID da pasta final (criando o que faltar).
    """
    parent = None
    for nome in caminho_pastas:
        pasta = _localizar_ou_criar_pasta(drive, nome, parent_id=parent['id'] if parent else None)
        parent = pasta
    return parent['id']

# ------------------------------------------------------------
# Upload CSV (sem arquivo temporário -> evita WinError 32)
# ------------------------------------------------------------
def _buscar_arquivo_por_titulo(drive, pasta_id, titulo):
    """Retorna o primeiro arquivo com título exato dentro de pasta_id (ou None)."""
    query = f"'{pasta_id}' in parents and title = '{titulo}' and trashed = false"
    itens = drive.ListFile({'q': query}).GetList()
    return itens[0] if itens else None

def _normalizar_nome_csv(nome_base: str) -> str:
    # Garante extensão .csv única
    base, ext = os.path.splitext(nome_base)
    return f"{base}.csv"

def salvar_df_csv_no_drive(
    df: pd.DataFrame,
    nome_base: str,
    sep: str = ",",
    encoding: str = "utf-8-sig",
    index: bool = False,
    sobrescrever: bool = False,
    versionar_timestamp: bool = False,
    subpastas: list[str] = None,
):
    """
    Sobe o CSV direto da memória usando SetContentString (sem arquivo temporário),
    mantendo sua lógica de:
      - localizar/criar pastas
      - sobrescrever arquivo
      - versionar por timestamp
      - criar _v{n} quando não versionar e não sobrescrever
    """
    if subpastas is None:
        subpastas = ["Adiantamento_APP", "bases_app"]

    drive = conectar_drive()
    pasta_final_id = _garantir_caminho_pastas(drive, subpastas)

    # Define título alvo
    nome_csv = _normalizar_nome_csv(nome_base)

    if versionar_timestamp:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        titulo = _normalizar_nome_csv(f"{os.path.splitext(nome_base)[0]}_{ts}")
    else:
        titulo = nome_csv  # p.ex. "df_p3_ajustado.csv"

    # Se não versionar e não sobrescrever, incrementa _v{n}
    if not versionar_timestamp and not sobrescrever:
        existente = _buscar_arquivo_por_titulo(drive, pasta_final_id, titulo)
        v = 1
        while existente:
            titulo = _normalizar_nome_csv(f"{os.path.splitext(nome_base)[0]}_v{v}")
            existente = _buscar_arquivo_por_titulo(drive, pasta_final_id, titulo)
            v += 1

    # Gera CSV em memória (string)
    # Obs: PyDrive ignora encoding aqui pois a string já está em memória;
    # se precisar de BOM/encoding específico, use bytes + SetContentString mesmo assim.
    csv_text = df.to_csv(index=index, sep=sep)

    # Sobrescrever mantendo o mesmo nome/ID (quando não versionar)
    if sobrescrever and not versionar_timestamp:
        alvo = _buscar_arquivo_por_titulo(drive, pasta_final_id, nome_csv)
        if alvo:
            f = drive.CreateFile({"id": alvo["id"]})
            f.SetContentString(csv_text)   # <<< sem arquivo temporário
            f.Upload()
            return {
                "file_id": f["id"],
                "file_title": f["title"],
                "folder_id": pasta_final_id,
                "path_display": "/".join(subpastas + [f["title"]]),
                "status": "updated",
            }

    # Criar novo arquivo (versionado ou novo nome)
    meta = {
        "title": titulo,
        "mimeType": "text/csv",
        "parents": [{"id": pasta_final_id}],
    }
    novo = drive.CreateFile(meta)
    novo.SetContentString(csv_text)
    novo.Upload()

    return {
        "file_id": novo["id"],
        "file_title": novo["title"],
        "folder_id": pasta_final_id,
        "path_display": "/".join(subpastas + [novo["title"]]),
        "status": "created",
    }
        
# ------------------------------------------------------------
# Exemplo de uso
# ------------------------------------------------------------
if __name__ == "__main__":
    # Exemplo de DataFrame
    df_exemplo = pd.DataFrame({
        "Pedido": ["4500001", "4500002"],
        "Valor": [1234.56, 7890.12],
        "Moeda": ["BRL", "BRL"],
    })

    # 1) Criar versão com timestamp (não sobrescreve nada):
    resp = salvar_df_csv_no_drive(
        df_exemplo,
        nome_base="resultado_consolidado",
        sep=";",                # se preferir ; para Excel/pt-BR
        encoding="utf-8-sig",   # inclui BOM; abre melhor no Excel
        index=False,
        versionar_timestamp=True
    )
    print(resp)

    # 2) Sobrescrever SEM versionar (mantém mesmo nome):
    resp2 = salvar_df_csv_no_drive(
        df_exemplo,
        nome_base="resultado_atual",
        sobrescrever=True,
        versionar_timestamp=False
    )
    print(resp2)
