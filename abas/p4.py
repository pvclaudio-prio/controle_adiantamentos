from dados.carga import filtros_iniciais
from dados.dados_teradata import df_teradata
from abas.p2 import tratar_bases_p2
import streamlit as st
import numpy as pd
import pandas as pd
from io import BytesIO
import numpy as np


@st.cache_data(show_spinner="Carregando dados...")
def carga_bases_p4():
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_rze_0600, df_baixas = filtros_iniciais()
    df_itens = df_teradata()
    
    return df_pm, df_in, df_rze, df_0600, df_itens, df_rze_0600, df_baixas

def tratar_bases_p4():
    df_pm, df_in, df_rze, df_0600, df_itens, df_rze_0600, df_baixas = carga_bases_p4()
    df_p2 = tratar_bases_p2()
    df_p2 = df_p2[["PO", "Valor Adt Lançado", "% Adt"]]
    
    df_itens_agrupado = df_itens.groupby("PurchaseOrder")["LineValue"].agg("sum").reset_index()
    df_itens.drop(columns="LineValue", inplace=True)
    df_itens = df_itens.merge(df_itens_agrupado, on="PurchaseOrder", how="left")
    
    df_pm = df_pm[df_pm["Doc.compensação"].isna()]
    df_pm = df_pm[df_pm["Documento de compras"].notna()]
    df_pm = df_pm[df_pm["Atribuição"].isna()]
    df_pm = df_pm[df_pm["Empresa"]!="0600"]
    df_pm["chave"] = df_pm["Documento de compras"]+df_pm["Item"]
    
    df_pm_agrupado = df_pm.groupby("chave")["Mont.moeda doc."].agg('sum').reset_index()
    
    df_p4 = df_pm.merge(df_itens[["chave","RequisitionerName","LineValue"]], on="chave", how="left")
    df_p4.rename(columns={"RequisitionerName":"Req/Comp"}, inplace=True)
    
    df_p4 = df_p4.drop_duplicates(subset="Documento de compras")
    
    df_p4.drop(columns="Mont.moeda doc.", inplace=True)
    df_p4 = df_p4.merge(df_pm_agrupado, on="chave", how="left")
    df_p4["LineValue"]  = df_p4["LineValue"].astype(float)
    df_p4["Mont.moeda doc."]  = df_p4["Mont.moeda doc."].astype(float)
    
    num = pd.to_numeric(df_p4["Mont.moeda doc."], errors="coerce")
    den = pd.to_numeric(df_p4["LineValue"],       errors="coerce")
    ratio = num.div(den).replace([np.inf, -np.inf], np.nan)
    df_p4["%"] = (ratio * 100).round(2) 
    
    
    mapa_colunas = {
        "Documento de compras":"PO",
        "Conta":"Cód Forn.",
        "Nome Fornecedor":"Fornecedor",
        "Doc.referência":"MIRO",
        "Moeda do documento":"Moeda",
        "Mont.moeda doc.":"Valor",
        "LineValue": "Valor Linha PO",
        "RequisitionerName":"Req/Comp"
        }
    
    df_p4 = df_p4.rename(columns=mapa_colunas)
    df_p4 = df_p4.merge(df_p2, on="PO", how="left")
    df_p4 = df_p4[["PO","Empresa","Cód Forn.","Fornecedor","Valor","Moeda","Valor Linha PO","%","Req/Comp","Valor Adt Lançado", "% Adt"]]
    df_p4["Valor Adt Lançado"] = df_p4["Valor Adt Lançado"].fillna(0)
    df_p4["% Adt"] = df_p4["% Adt"].fillna(0)
    
    df_0600.rename(columns={1200: "PO", 600: "PO 0600"}, inplace=True)
    df_p4 = df_p4.merge(df_0600, on="PO", how="left")

    # --- ADIÇÃO: criar flag e esconder a coluna 'PO 0600' ---
    df_p4["🚩"] = np.where(df_p4["PO 0600"].notna(), "🚩", "")
    df_p4.drop(columns=["PO 0600"], inplace=True, errors="ignore")
    # -------------------------------------------------------

    return df_p4

def layout_p4():
    df_p4 = tratar_bases_p4()

    # ---- visão apenas para exibir (não altera df_p4) ----
    df_view = df_p4.copy()

    # Descobre/gera a coluna de flag
    flag_col = None
    for c in ["🚩 0600", "🚩"]:
        if c in df_view.columns:
            flag_col = c
            break
    if flag_col is None and "PO 0600" in df_view.columns:
        df_view["🚩"] = np.where(df_view["PO 0600"].notna(), "🚩", "")
        flag_col = "🚩"
    # nunca mostrar "PO 0600" ao usuário
    df_view.drop(columns=["PO 0600"], inplace=True, errors="ignore")

    # PO com bandeira na visualização
    if flag_col:
        df_view["PO"] = np.where(
            df_view[flag_col] == "🚩",
            df_view["PO"].astype(str) + "  🚩",
            df_view["PO"].astype(str),
        )
        # máscara de linhas sinalizadas e REMOVER a coluna do flag da exibição
        flag_mask = df_view[flag_col].eq("🚩")
        df_view = df_view.drop(columns=[flag_col])

    # -------- formatação BR apenas para exibição --------
    def fmt_br(x, dec=2):
        if pd.isna(x):
            return ""
        try:
            return f"{float(x):,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return x

    fmt_cols = {
        "Valor": 2,
        "Valor Linha PO": 2,
        "Valor Adt Lançado": 2,
        "%": 2,
        "% Adt": 2,
    }
    fmt_map = {col: (lambda d: (lambda v: fmt_br(v, d)))(dec)
               for col, dec in fmt_cols.items() if col in df_view.columns}

    styler = df_view.style.format(fmt_map)

    # destacar a linha com base na máscara (sem precisar da coluna do flag)
    if 'flag_mask' in locals():
        styler = styler.apply(
            lambda row: ['background-color: #FFF3CD' if flag_mask.loc[row.name] else '' for _ in row],
            axis=1
        )

    st.dataframe(styler, use_container_width=True)

    st.markdown(f'A base possui **{df_p4.shape[0]}** linhas e **{df_p4.shape[1]}** colunas.')

    # exporta o ORIGINAL (sem emoji na PO); mantém como está
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_p4.to_excel(writer, index=False, sheet_name="Pgtos em aberto")

    st.download_button(
        label="⬇️ Baixar base (.xlsx)",
        data=buffer.getvalue(),
        file_name=f"pgto_em_aberto_{pd.Timestamp.today().strftime('%Y-%m-%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=False,
    )
