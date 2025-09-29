from dados.carga import filtros_iniciais
from dados.dados_teradata import df_teradata
from abas.p3 import tratar_bases_p3
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from dados.upload import ler_df_csv_do_drive
from dados.salvar_bases import salvar_df_csv_no_drive

@st.cache_data(show_spinner="Carregando dados...")
def carga_bases_p2():
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_rze_0600, df_baixas = filtros_iniciais()
    df_itens = df_teradata()
    df_p3_compensada, df_p3_aberta = tratar_bases_p3()
    
    return df_pm, df_in, df_rze, df_0600, df_expurgo, df_itens, df_p3_compensada, df_p3_aberta, df_rze_0600, df_baixas

def tratar_bases_p2():
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_itens, df_p3_compensada, df_p3_aberta, df_rze_0600, df_baixas = carga_bases_p2()
    
    lista_expurgo = df_expurgo["Documento de compras"].unique().tolist()
    
    df_baixas.rename(columns={"PO Impactada": "PO"}, inplace=True)
    
    df_rze = df_rze[df_rze["Documento de compras"].notna()]
    df_rze = df_rze[~df_rze["Documento de compras"].isin(lista_expurgo)]
    df_rze_aberto = df_rze[df_rze["Doc.compensação"].isna()]
    df_rze_pago = df_rze[df_rze["Nº documento"].astype(str).str[:2].isin(["15","18","19","20","48"])]
    df_rze_0600 = df_rze_0600[df_rze_0600["Nº documento"].astype(str).str[:2].isin(["15","18","19","20","48"])]
    
    df_pm = df_pm[df_pm["Documento de compras"].notna()]
    df_pm_pago = df_pm[df_pm["Doc.compensação"].astype(str).str[:2].isin(["15","18","19","20","48"])]
    df_pm_pago = df_pm_pago[df_pm_pago["Empresa"]!="0600"]
    df_pm_pago_0600 = df_pm[df_pm["Doc.compensação"].astype(str).str[:2].isin(["15","18","19","20","48"])]
    df_pm_pago_0600 = df_pm_pago_0600[df_pm_pago_0600["Empresa"]=="0600"]
    
    mapa_colunas_rze_aberto = {
        "Documento de compras":"PO",
        "Conta":"Cód Forn.",
        "Nome Fornecedor":"Fornecedor",
        "Doc.referência":"MIRO",
        "Moeda do documento":"Moeda",
        "Mont.moeda doc.":"Adt Em Aberto",
        "Montante em moeda interna":"Adt em aberto BRL"
        }
    
    mapa_colunas_rze_pago = {
        "Documento de compras":"PO",
        "Conta":"Cód Forn.",
        "Nome Fornecedor":"Fornecedor",
        "Doc.referência":"MIRO",
        "Moeda do documento":"Moeda (Adt Pago)",
        "Mont.moeda doc.":"Valor Adt Pago"
        }
    
    mapa_colunas_pm = {
        "Documento de compras":"PO",
        "Conta":"Cód Forn.",
        "Nome Fornecedor":"Fornecedor",
        "Doc.referência":"MIRO",
        "Moeda do documento":"Moeda (Adt Lançado)",
        "Mont.moeda doc.":"Valor Adt Lançado"
        }
    
    mapa_colunas_pm_0600 = {
        "Documento de compras":"PO",
        "Conta":"Cód Forn.",
        "Nome Fornecedor":"Fornecedor",
        "Doc.referência":"MIRO",
        "Moeda do documento":"Moeda (Adt Lançado) 0600",
        "Mont.moeda doc.":"Valor Adt Lançado 0600"
        }
    
    df_rze.rename(columns=mapa_colunas_rze_aberto, inplace=True)
    df_rze_aberto.rename(columns=mapa_colunas_rze_aberto, inplace=True)
    df_rze_aberto_agrupado = df_rze_aberto.groupby(["PO","Moeda"])["Adt Em Aberto"].agg("sum").reset_index()
    df_rze_aberto_agrupado.rename(columns={"Moeda": "Moeda (Adt em aberto)"}, inplace=True)
    
    df_rze_aberto_agrupado_brl = df_rze_aberto.groupby(["PO"])["Adt em aberto BRL"].agg("sum").reset_index()
    
    df_tipo_material = df_itens[["PurchaseOrder","PurchaseOrderItem","Material"]]
    df_tipo_material["tipo"] = df_tipo_material["Material"].str[:1]
    df_tipo_material["tipo"] = np.where(df_tipo_material["tipo"] == "8", "Serviço", "Material")
    
    # Ajusta para casos com os dois tipos no mesmo PurchaseOrder
    df_tipo_material = (
        df_tipo_material
        .groupby("PurchaseOrder", as_index=False)
        .agg({"tipo": lambda x: "/".join(sorted(set(x)))})
    )
    df_tipo_material.rename(columns={"PurchaseOrder":"PO"},inplace=True)
    
    df_itens_agrupado = df_itens.groupby("PurchaseOrder")["LineValue"].agg("sum").reset_index()
    df_itens_agrupado = df_itens_agrupado.rename(columns={"PurchaseOrder":"PO"})
    
    df_itens_agrupado2 = df_itens.groupby("PurchaseOrder")["LineValue2"].agg("sum").reset_index()
    df_itens_agrupado2 = df_itens_agrupado2.rename(columns={"PurchaseOrder":"PO"})
    
    df_entrega_max = (
        df_itens
        .dropna(subset=["ScheduleLineDeliveryDate"])
        .groupby("PurchaseOrder", as_index=False)["ScheduleLineDeliveryDate"]
        .max()
    )
    
    df_itens.drop(columns=["ScheduleLineDeliveryDate"], inplace=True)
    df_itens = df_itens.merge(df_entrega_max[["PurchaseOrder","ScheduleLineDeliveryDate"]], on="PurchaseOrder", how="left")
    
    df_itens = df_itens.rename(columns={"PurchaseOrder":"PO","DocumentCurrency":"Moeda PO","ScheduleLineDeliveryDate":"Data remessa SAP"})

    df_pm_pago = df_pm_pago.rename(columns=mapa_colunas_pm)    
    df_pm_pago_0600 = df_pm_pago_0600.rename(columns=mapa_colunas_pm_0600) 
    df_pm_pago_agrupado = df_pm_pago.groupby(["PO","Moeda (Adt Lançado)"])["Valor Adt Lançado"].agg("sum").reset_index()
    df_pm_pago_agrupado_0600 = df_pm_pago_0600.groupby(["PO","Moeda (Adt Lançado) 0600"])["Valor Adt Lançado 0600"].agg("sum").reset_index()
    
    df_pm_pago["chave_rze"] = df_pm_pago["PO"]+df_pm_pago["Doc.compensação"]
    df_pm_pago_0600["chave_rze"] = df_pm_pago_0600["PO"]+df_pm_pago_0600["Doc.compensação"]
    
    df_rze_pago.rename(columns=mapa_colunas_rze_pago, inplace=True)
    df_rze_pago["chave_rze"] = df_rze_pago["PO"]+df_rze_pago["Nº documento"]
    
    df_rze_0600.rename(columns=mapa_colunas_rze_pago, inplace=True)
    df_rze_0600["chave_rze_0600"] =df_rze_0600["PO"]+df_rze_0600["Nº documento"]
    df_rze_0600.rename(columns={"PO":"PO 0600"}, inplace=True)
    
    lista_pm = np.union1d(
    df_pm_pago["chave_rze"].dropna().astype(str).str.strip(),
    df_pm_pago_0600["chave_rze"].dropna().astype(str).str.strip()).tolist()
    
    df_rze_pago = df_rze_pago[df_rze_pago["chave_rze"].isin(lista_pm)]
    df_rze_pago_agrupado = df_rze_pago.groupby(["PO","Moeda (Adt Pago)"])["Valor Adt Pago"].agg("sum").reset_index()
    
    df_rze_0600 = df_rze_0600[df_rze_0600["chave_rze_0600"].isin(lista_pm)]
    df_rze_0600 = df_rze_0600.groupby(["PO 0600","Moeda (Adt Pago)"])["Valor Adt Pago"].agg("sum").reset_index()
    df_rze_0600 = df_rze_0600.rename(columns={"Moeda (Adt Pago)": "Moeda (Adt Pago 0600)", "Valor Adt Pago": "Valor Adt Pago 0600"})
    
    df_p3_compensada_agrupado = df_p3_compensada.groupby(["PO","Moeda"])[["Pago","Compensado"]].agg("sum").reset_index()
    df_p3_compensada_agrupado_0600 = df_p3_compensada_agrupado.copy()
    df_p3_compensada_agrupado_0600.rename(columns={"Compensado":"MIRO Compensada 0600","Pago":"MIRO Paga 0600"}, inplace=True)
    df_p3_compensada_agrupado_0600.drop(columns={"Moeda"},inplace=True)
    df_p3_compensada_agrupado.rename(columns={"Compensado":"MIRO Compensada","Pago":"MIRO Paga",
                                              "Moeda":"Moeda MIRO"}, inplace=True)
    
    df_p2 = df_rze["PO"].copy()
    df_p2 = pd.DataFrame(df_p2)
    df_p2 = df_p2.drop_duplicates("PO")
    df_p2 = df_p2.merge(df_0600,left_on="PO",right_on=1200,how="left")
    df_p2.drop(columns={1200},inplace=True)
    df_p2 = df_p2.rename(columns={600:"PO 0600"})
    df_p2["PO 0600"] = df_p2["PO 0600"].fillna("Não possui 0600")
    df_p2 = df_p2.merge(df_rze[["PO","Empresa","Cód Forn.","Fornecedor"]],on="PO",how="left")
    df_p2 = df_p2.drop_duplicates("PO")
    
    df_p2 = df_p2.merge(df_tipo_material,on="PO",how="left")
    df_p2 = df_p2.merge(df_itens_agrupado,on="PO",how="left")
    
    #Ajuste para verificar o melhor campo de valor
    df_p2 = df_p2.merge(df_itens_agrupado2[["PO","LineValue2"]],on="PO",how="left")
    df_p2  =df_p2.rename(columns={"LineValue2": "Valor PO NET"})
    
    df_p2 = df_p2.merge(df_itens[["PO","Moeda PO","Data remessa SAP"]],on="PO",how="left")
    df_p2 = df_p2.drop_duplicates("PO")
    
    df_p2 = df_p2.merge(df_itens_agrupado,left_on="PO 0600",right_on="PO",how="left")
    df_p2 = df_p2.drop(columns="PO_y")
    df_p2 = df_p2.rename(columns={"PO_x":"PO","LineValue_x":"Valor PO","LineValue_y":"Valor PO 0600"})
    df_p2["Valor PO 0600"] = df_p2["Valor PO 0600"].fillna(0)
    df_p2["Data Remessa Atual"] = df_p2["Data remessa SAP"]
    
    #Ajuste para verificar o melhor campo de valor
    df_p2 = df_p2.merge(df_itens_agrupado2[["PO","LineValue2"]],left_on="PO 0600",right_on="PO",how="left")
    df_p2 = df_p2.drop(columns="PO_y")
    df_p2 = df_p2.rename(columns={"PO_x":"PO","LineValue2":"Valor PO 0600 NET"})
    df_p2["Valor PO 0600 NET"] = df_p2["Valor PO 0600 NET"].fillna(0)
    
    df_p2 = df_p2.merge(df_rze_aberto_agrupado,on="PO",how="left")
    
    df_p2 = df_p2.merge(df_pm_pago_agrupado,on="PO",how="left")
    df_p2 = df_p2.merge(df_pm_pago_agrupado_0600,left_on="PO 0600", right_on="PO", how="left")
    df_p2 = df_p2.drop(columns="PO_y")
    df_p2 = df_p2.rename(columns={"PO_x":"PO"})
    
    df_p2 = df_p2.merge(df_rze[["PO","Doc.compensação"]],on="PO",how="left")
    df_p2 = df_p2.drop_duplicates(["PO","Moeda (Adt em aberto)"])
    df_p2 = df_p2.merge(df_rze_pago_agrupado,on="PO",how="left")
    df_p2 = df_p2.merge(df_rze_0600,on="PO 0600",how="left")
    df_p2 = df_p2.drop_duplicates(["PO","Adt Em Aberto"])
    
    num = pd.to_numeric(df_p2["Valor Adt Lançado"], errors="coerce")
    den = pd.to_numeric(df_p2["Valor PO"],errors="coerce")
    ratio = num.div(den).replace([np.inf, -np.inf], np.nan)
    df_p2["% Adt"] = (ratio * 100).round(2) 
    
    df_p2["Saldo PO Adt"] = df_p2["Valor PO"] - df_p2["Valor Adt Lançado"]
    df_p2 = df_p2.merge(df_p3_compensada_agrupado,on="PO",how="left")
    df_p2 = df_p2.merge(df_p3_compensada_agrupado_0600,left_on="PO 0600",right_on="PO",how="left")
    df_p2 = df_p2.rename(columns={"PO_x":"PO"})
    df_p2 = df_p2.drop(columns="PO_y")
    df_p2 = df_p2.drop_duplicates(["PO","MIRO Paga 0600","MIRO Compensada 0600"])
    df_p2["Saldo PO MIRO"] = df_p2["Valor PO"] - (df_p2["MIRO Paga"]-df_p2["MIRO Compensada"])
    df_p2["Status"] = ""
    df_p2["Motivo"] = ""
    df_p2["Responsável"] = ""
    df_p2["Comentários"] = ""

    df_p2 = df_p2.merge(df_rze_aberto_agrupado_brl, on="PO", how="left")
    df_p2 = df_p2.merge(df_baixas, on="PO", how="left")
    df_p2["Adt Em Aberto"] = df_p2["Adt Em Aberto"].fillna(0)
    df_p2["Valor Adt Lançado"] = df_p2["Valor Adt Lançado"].fillna(0)
    df_p2["Valor Adt Lançado 0600"] = df_p2["Valor Adt Lançado 0600"].fillna(0)
    df_p2["Valor Adt Pago"] = df_p2["Valor Adt Pago"].fillna(0)
    df_p2["Valor Adt Pago 0600"] = df_p2["Valor Adt Pago 0600"].fillna(0)
    df_p2["% Adt"] = df_p2["% Adt"].fillna(0)
    df_p2["Saldo PO Adt"] = df_p2["Saldo PO Adt"].fillna(0)
    df_p2["MIRO Compensada"] = df_p2["MIRO Compensada"].fillna(0)
    df_p2["MIRO Compensada 0600"] = df_p2["MIRO Compensada 0600"].fillna(0)
    df_p2["MIRO Paga"] = df_p2["MIRO Paga"].fillna(0)
    df_p2["MIRO Paga 0600"] = df_p2["MIRO Paga 0600"].fillna(0)
    df_p2["Saldo PO MIRO"] = df_p2["Saldo PO MIRO"].fillna(0)
    df_p2["Moeda (Adt Lançado)"] = df_p2["Moeda (Adt Lançado)"].fillna(df_p2["Moeda (Adt Lançado) 0600"])
    df_p2["Moeda (Adt Lançado) 0600"] = df_p2["Moeda (Adt Lançado) 0600"].fillna(df_p2["Moeda (Adt Lançado)"])
    df_p2["Moeda (Adt Pago)"] = df_p2["Moeda (Adt Pago)"].fillna(df_p2["Moeda (Adt Pago 0600)"])
    df_p2["Moeda (Adt Pago 0600)"] = df_p2["Moeda (Adt Pago 0600)"].fillna(df_p2["Moeda (Adt Pago)"])
    df_p2["Moeda MIRO"] = df_p2["Moeda MIRO"].fillna(df_p2["Moeda PO"])
    df_p2["Doc.compensação"] = df_p2["Doc.compensação"].fillna(0)
    df_p2["Doc.compensação"] = df_p2["Doc.compensação"].astype(str)
    df_p2["Doc.compensação"] = df_p2["Doc.compensação"].replace({"0":"não possui"})
    df_p2["Forma baixa"] = df_p2["Forma baixa"].fillna("n/i")
    df_p2["N° Título SAP"] = df_p2["N° Título SAP"].fillna("n/i")
    df_p2["Valor título"] = df_p2["Valor título"].fillna(0)
    df_p2["Valor Utilizado"] = df_p2["Valor Utilizado"].fillna(0)
    df_p2["Moeda"] = df_p2["Moeda"].fillna("n/i")
    
    df_p2.drop(columns=["Moeda (Adt Lançado)", "Moeda (Adt Lançado) 0600", "Moeda MIRO"], inplace=True)
    
    return df_p2

def layout_p2():
    
    #st.dataframe(df_p2)
    #st.markdown(f'A base possui **{df_p2.shape[0]}** linhas e **{df_p2.shape[1]}** colunas.')
        
    EDITABLE_COLS = ["Status", "Motivo", "Responsável", "Comentários"]
    STRING_COLS = ["PO", "Empresa", "Cód Forn.", "Doc.compensação"]
    STATUS_OPTIONS = [
    "Finalizado",
    "Avaliar",
    "Dentro do Prazo",
    "Analisando",
    "Gestão de notas",
    "Solicitar devolução",
    "Ag Fornecedor",
    "Solicitar Logistica",
    "Ag Logistica",
    "Solicitar Comex",
    "Ag Comex",
    "Solicitar Suprimentos",
    "Ag Suprimentos"
]
    MOTIVO_OPTIONS = [
    "Pgto Maior tesouraria",
    "Adt > Saldo PO",
    "Atraso entrega",
    "Serviço não realizado",
    "Material não entregue",
    "Triangulação",
    "Projeto on Hold",
    "MIRO pendente",
    "MIGO pendente",
    "PO com erro",
    "PO não migrada",
    "PO não migrada, com saldo pend"
]
    RESPONSAVEIS_OPTIONS = [
    "Amanda Vargas",
    "Bruna Carolina Soares",
    "Bruno Tamiozo",
    "Caio Saraiva",
    "George Pinto",
    "Giovana Virgolino",
    "Guilherme Oliveira",
    "Isaías Simões",
    "Jose Augusto Filho",
    "Jose Guilherme Azambuja",
    "Leonardo Espindola",
    "Lucas Prudencio",
    "Maycon Lucas",
    "Monique Silva",
    "Monique Correia",
    "Nathan Bastos",
    "Nayara Dias",
    "Pedro Henrique Varela",
    "Raíssa Farias",
    "Rodrigo Coutinho",
    "Rodrigo Goncalves",
    "Thiago Rocha",
    "Vanessa Pessin",
    "Isadora Poubel",
    "Daniela Faria",
    "Yuri de Oliveira"
]
    NOME_BASE = "df_p2_ajustado.csv"
    
    @st.cache_data(show_spinner=False)
    def carregar_base(nome_base: str, sep=","):
        # Sua função original: ler_df_csv_do_drive(...)
        df = ler_df_csv_do_drive(nome_base, sep=sep)
            
        return df
    
    def salvar_base_no_drive(df: pd.DataFrame, nome_base: str, sobrescrever=True, versionar_timestamp=False):
        # Sua função original: salvar_df_csv_no_drive(...)
        salvar_df_csv_no_drive(
            df,
            nome_base=nome_base,
            sobrescrever=sobrescrever,
            versionar_timestamp=versionar_timestamp
        )
    
    def normalizar_tipos_para_modelo(df: pd.DataFrame) -> pd.DataFrame:
        """Garante tipos corretos para a base persistente (sem formatar datas como string)."""
        df = df.copy()
    
        # Exemplo: tentar converter datas mantendo datetime
        for col in ["Data remessa SAP", "Data Remessa Atual"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
    
        # Padronizar colunas editáveis como string (sem 'n/i' aqui) e strip
        for col in EDITABLE_COLS:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype("string")  # permite NaN
                    .str.strip()
                    .replace({"": pd.NA})
                )
    
        # Validar Status contra lista permitida (se não pertencer, setar NA)
        if "Status" in df.columns:
            df["Status"] = df["Status"].where(df["Status"].isin(STATUS_OPTIONS), pd.NA)
    
        return df
    
    def formatar_para_exibicao(df: pd.DataFrame) -> pd.DataFrame:
        """Cria uma cópia apenas para exibição no data_editor (datas formatadas e 'n/i')."""
        view = df.copy()
    
        # Formatar datas apenas para visual
        for col in ["Data remessa SAP", "Data Remessa Atual"]:
            if col in view.columns:
                view[col] = pd.to_datetime(view[col], errors="coerce").dt.strftime("%d/%m/%Y")
                view[col] = view[col].fillna("")  # vazio na tela
    
        # Preencher campos textuais na tela com 'n/i' apenas para conforto visual
        for col in EDITABLE_COLS:
            if col in view.columns:
                view[col] = view[col].fillna("n/i")
        
        for col in STRING_COLS:
            view[col] = view[col].astype(str)
            
        return view
    
    def limpar_dados_pos_edicao(df_view_editado: pd.DataFrame, df_base_original: pd.DataFrame) -> pd.DataFrame:
        """
        Recebe o DF retornado do data_editor (formatado pra visual) e reconcilia com a base original,
        preservando tipos e limpando placeholders.
        """
        df = df_view_editado.copy()
    
        # Remover placeholder 'n/i' das colunas editáveis -> NaN
        for col in EDITABLE_COLS:
            if col in df.columns:
                df[col] = df[col].replace({"n/i": pd.NA}).astype("string").str.strip().replace({"": pd.NA})
    
        # Reverter datas exibidas como 'dd/mm/yyyy' para datetime (se o usuário não alterou, ok)
        for col in ["Data remessa SAP", "Data Remessa Atual"]:
            if col in df.columns:
                # Se o usuário mexeu nessas colunas (mesmo travadas, só por garantia)
                # converte; senão, vamos re-aplicar da base original
                try:
                    conv = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
                    # Se muitas datas viraram NaT, preferir original
                    if conv.notna().mean() < 0.5 and col in df_base_original.columns:
                        df[col] = df_base_original[col]
                    else:
                        df[col] = conv
                except Exception:
                    if col in df_base_original.columns:
                        df[col] = df_base_original[col]
    
        # Garantir tipos finais
        df = normalizar_tipos_para_modelo(df)
        return df
    
    def hash_df_basico(df: pd.DataFrame) -> str:
        """Hash simples para evitar salvar se nada mudou."""
        return pd.util.hash_pandas_object(df.fillna(""), index=True).sum().astype(str)
    
    # -----------------------------
    # Reprocessamento opcional
    # -----------------------------
    if st.button("🔄 Reprocessar bases?"):
        with st.spinner("Reprocessando..."):
            df_p2 = tratar_bases_p2()
            df_p2 = normalizar_tipos_para_modelo(df_p2)
            salvar_base_no_drive(df_p2, NOME_BASE, sobrescrever=True, versionar_timestamp=False)
            st.success("Base reprocessada e salva.")
            st.cache_data.clear()  # limpar cache para recarregar
    
    # -----------------------------
    # Carregar base e preparar exibição
    # -----------------------------
    df_p2_base = carregar_base(NOME_BASE, sep=",")
    df_p2_base = normalizar_tipos_para_modelo(df_p2_base)
    df_view = formatar_para_exibicao(df_p2_base)
    
    aberto = st.toggle("Visualizar abertos apenas?")
    
    if aberto:
        df_view = df_view[df_view["Doc.compensação"]=="não possui"]
        
    # Guardar hash original na sessão
    if "hash_original" not in st.session_state:
        st.session_state.hash_original = hash_df_basico(df_p2_base)
    
    # -----------------------------
    # Edição em formulário (evita salvar a cada rerun)
    # -----------------------------
    with st.form("form_edicao"):
        df_p2_editado_view = st.data_editor(
            df_view,
            column_config={
                "Status": st.column_config.SelectboxColumn(
                    "Status",
                    options=STATUS_OPTIONS,
                    required=False,
                    help="Selecione o status"
                ),
                "Motivo": st.column_config.SelectboxColumn(
                    "Motivo",
                    options=MOTIVO_OPTIONS,
                    required=False,
                    help="Selecione o motivo"
                ),
                "Responsável": st.column_config.SelectboxColumn(
                    "Responsável",
                    options=RESPONSAVEIS_OPTIONS,
                    required=False,
                    help="Selecione o responsável"
                ),
                "Comentários": st.column_config.TextColumn("Comentários", width="large"),
            },
            disabled=[c for c in df_view.columns if c not in EDITABLE_COLS],
            hide_index=True,
            num_rows="fixed",
            use_container_width=True,
            height=min(600, 44 + 35 * min(15, len(df_view)))  # ajuste estético opcional
        )
    
        col_a, col_b = st.columns([1, 2])
        salvar_click = col_a.form_submit_button("💾 Salvar alterações", use_container_width=True)
        baixar_click = col_b.form_submit_button("⬇️ Baixar base (.xlsx)", use_container_width=True)
    
    st.write(f"A base possui **{df_view.shape[0]}** linhas e **{df_view.shape[1]}** colunas.")
    
    # -----------------------------
    # Ações do formulário
    # -----------------------------
    if baixar_click:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_p2_editado_view.to_excel(writer, index=False, sheet_name="Dados")
    
        buffer.seek(0)
    
        st.download_button(
            label="⬇️ Baixar base (.xlsx)",
            data=buffer,  # pode ser buffer ou buffer.getvalue()
            file_name=f"dados_adiantamentos_{pd.Timestamp.today().strftime('%Y-%m-%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
            key="download_base_xlsx",  # evite recriação do widget
        )
    
    if salvar_click:
        with st.spinner("Validando e salvando..."):
            # Reconcilia view -> base
            df_p2_ajustado = limpar_dados_pos_edicao(df_p2_editado_view, df_p2_base)
            novo_hash = hash_df_basico(df_p2_ajustado)
    
            if novo_hash == st.session_state.hash_original:
                st.info("Nenhuma alteração detectada. Nada foi salvo.")
            else:
                # (Opcional) salvar backup com timestamp antes de sobrescrever
                # salvar_base_no_drive(df_p2_ajustado, f"backup/df_p2_ajustado_{datetime.now():%Y%m%d_%H%M%S}.csv", sobrescrever=True, versionar_timestamp=False)
    
                salvar_base_no_drive(df_p2_ajustado, NOME_BASE, sobrescrever=True, versionar_timestamp=False)
                st.session_state.hash_original = novo_hash
                st.success("Alterações salvas no Drive com sucesso.")
                st.cache_data.clear()

    
