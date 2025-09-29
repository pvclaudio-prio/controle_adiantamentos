# -*- coding: utf-8 -*-
# ============================================================
# Ler CSV do Google Drive em Adiantamentos_APP/bases_app
# ============================================================
import os
import pandas as pd
import tempfile
from dados.ingestao import conectar_drive
from dados.salvar_bases import _garantir_caminho_pastas

def ler_df_csv_do_drive(
    nome_arquivo: str,
    sep: str = ",",
    encoding: str = "utf-8-sig",
    subpastas: list[str] = None,
    read_kwargs: dict = None,
) -> pd.DataFrame:
    """
    Lê um arquivo CSV salvo no Google Drive e retorna como DataFrame.
    
    Parâmetros:
      - nome_arquivo: ex.: 'df_p2.csv'
      - sep: separador (padrão ',')
      - encoding: encoding do arquivo (padrão 'utf-8-sig')
      - subpastas: caminho das pastas; padrão ['Adiantamento_APP', 'bases_app']
      - read_kwargs: kwargs extras para pandas.read_csv
    
    Retorna:
      DataFrame lido do CSV.
    """
    if subpastas is None:
        subpastas = ["Adiantamento_APP", "bases_app"]
    if read_kwargs is None:
        read_kwargs = {}

    # Conecta ao Drive
    drive = conectar_drive()

    # Garante pasta final
    pasta_final_id = _garantir_caminho_pastas(drive, subpastas)

    # Procura o arquivo
    query = f"'{pasta_final_id}' in parents and title = '{nome_arquivo}' and trashed = false"
    itens = drive.ListFile({'q': query}).GetList()
    if not itens:
        raise FileNotFoundError(f"Arquivo '{nome_arquivo}' não encontrado em {'/'.join(subpastas)}")

    # Baixa temporário
    tmp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
    itens[0].GetContentFile(tmp_path)

    try:
        df = pd.read_csv(tmp_path, sep=sep, encoding=encoding, **read_kwargs)
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

    return df


# ------------------------------------------------------------
# Exemplo de uso
# ------------------------------------------------------------
if __name__ == "__main__":
    df_p2 = ler_df_csv_do_drive("df_p2.csv", sep=",")
    print(df_p2.head())
