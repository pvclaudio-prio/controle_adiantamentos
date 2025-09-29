from dados.ingestao import carregar_bases_adiantamento
import streamlit as st

@st.cache_data(show_spinner="Carregando dados das bases...")
def carregamento_bases():
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_baixas = carregar_bases_adiantamento(
        sheets={"pm": 0, "in": 0, "rze": 0, "0600": 0, "expurgo": 0, "inpago": 0, "baixas": 0}
    )
    
    return df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_baixas


#Filtros Iniciais

def filtros_iniciais():
    
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_baixas = carregamento_bases()
    
    #Ajustes df_pm
    df_pm["Item"] = df_pm["Item"].astype(int).astype(str)
    df_pm = df_pm.rename(columns={"Data de lançamento":"Data de entrada"})
    df_pm = df_pm[~df_pm["Empresa"].isin(["0500","0800","0900","1100","1300"])]
    df_pm["Mont.moeda doc."] = df_pm["Mont.moeda doc."].astype(float)
    df_pm["Montante em moeda interna"] = df_pm["Montante em moeda interna"].astype(float)
    df_pm["Mont.moeda doc."] = df_pm["Mont.moeda doc."]*-1
    df_pm["Montante em moeda interna"] = df_pm["Montante em moeda interna"]*-1
    
    #Ajustes df_rze
    df_rze["Item"] = df_rze["Item"].astype(int).astype(str)
    df_rze = df_rze.rename(columns={"Data de lançamento":"Data de entrada"})
    df_rze = df_rze[df_rze["Conta"]!="1005726"]
    df_rze = df_rze[~df_rze["Conta"].astype(str).str[:1].isin(["P","J"])]
    df_rze["Mont.moeda doc."] = df_rze["Mont.moeda doc."].astype(float)
    df_rze["Montante em moeda interna"] = df_rze["Montante em moeda interna"].astype(float)
    df_rze_0600 = df_rze.copy()
    df_rze = df_rze[~df_rze["Empresa"].isin(["0500","0600","0800","0900","1100","1300"])]
    df_rze_0600 = df_rze_0600[df_rze_0600["Empresa"].isin(["0600"])]
    
    #Ajustes df_in
    df_in = df_in.rename(columns={"Item doc.compra":"Item","Fornecedor":"Conta","Mont.(moeda trans)":"Mont.moeda doc.","Moeda da transação":"Moeda do documento",
                                  "Montante (ME)":"Montante em moeda interna","Moeda da empresa":"Moeda interna","Lançamento contábil":"Nº documento",
                                  "Lançto.compensação":"Doc.compensação","Inserido em":"Data de lançamento","Data vencimento líq.":"Vencimento líquido",
                                  "Texto de item":"Texto","Documento de referência original":"Doc.referência","Nome do fornecedor":"Nome Fornecedor"})
    df_in["Item"] = df_in["Item"].astype(int).astype(str)
    df_in = df_in[df_in["Nº documento"].astype(str).str[:2].isin(["51"])]
    df_in = df_in[df_in["Conta"]!="1005726"]
    df_in = df_in[~df_in["Conta"].astype(str).str[:1].isin(["P","J"])]
    df_in["Mont.moeda doc."] = df_in["Mont.moeda doc."].astype('float')
    df_in["Montante em moeda interna"] = df_in["Montante em moeda interna"].astype(float)
    df_in["Mont.moeda doc."] = df_in["Mont.moeda doc."]*-1
    df_in["Montante em moeda interna"] = df_in["Montante em moeda interna"]*-1
    df_in["Referência"] = df_in["Referência"].str.strip()
    
    df_inpago = df_inpago[["Documento de compras","Documento de referência original","Pago","Baixado","Estorno"]]
    df_inpago = df_inpago.rename(columns={"Baixado":"Compensado"})
    df_inpago["Pago"] = df_inpago["Pago"].astype(float)*-1
    df_inpago["Compensado"] = df_inpago["Compensado"].astype(float)*-1
    df_inpago["Estorno"] = df_inpago["Estorno"].astype(float)
    df_inpago["Documento de referência original"] = df_inpago["Documento de referência original"].str.strip()
    df_inpago.rename(columns={"Documento de referência original":"Doc.referência"}, inplace=True)
    
    return df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_rze_0600, df_baixas
    
