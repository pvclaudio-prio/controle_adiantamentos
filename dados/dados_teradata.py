import teradatasql
from dotenv import load_dotenv
import os
import streamlit as st
import pandas as pd

load_dotenv()

# Credenciais de conexão

HOST = os.getenv("DB_HOST")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
SCHEMA = "AA_PRD_DDM"
SCHEMA2 = "AA_PRD_WRK"

@st.cache_data(show_spinner="Carregando dados do Teradata...")
def base_teradata():
    try:
        with teradatasql.connect(host=HOST, user=USER, password=PASSWORD) as conn:
                    print("Conexão bem-sucedida!")
                    
                    with conn.cursor() as cur:
                        
                        query_itens = f"""
                            SELECT
                                "PurchaseOrder",
                                "PurchaseOrderItem",
                                "PurchaseOrderCategory",
                                "DocumentCurrency",
                                "MaterialGroup",
                                "Material",
                                "MaterialType",
                                "PurchaseOrderItemText",
                                "CompanyCode",
                                "IsFinallyInvoiced",
                                "NetAmount",
                                "GrossAmount",
                                "EffectiveAmount",
                                "NetPriceAmount",
                                "OrderQuantity",
                                "NetPriceQuantity",
                                "PurgDocPriceDate",
                                "PurchaseRequisition",
                                "RequisitionerName",
                                "PurchaseContract",
                                "AccountAssignmentCategory"
                            FROM {SCHEMA2}.I_PurchaseOrderItemAPI01
                            WHERE "PurchasingDocumentDeletionCode" = ''
                        """
                        cur.execute(query_itens)
                        columns_itens = [desc[0] for desc in cur.description]
                        df_itens = pd.DataFrame(cur.fetchall(), columns=columns_itens)
                        
                        query_entrega= f"""
                            SELECT
                                "PurchaseOrder",
                                "PurchaseOrderItem",
                                "ScheduleLineDeliveryDate"
                            FROM {SCHEMA2}.I_PurOrdScheduleLineAPI01
                        """
                        cur.execute(query_entrega)
                        columns_entrega = [desc[0] for desc in cur.description]
                        df_entrega = pd.DataFrame(cur.fetchall(), columns=columns_entrega)
                        
                        query_compradores = f"""
                            SELECT
                                "PurchasingGroup",
                                "PurchasingGroupName"
                            FROM {SCHEMA2}.I_PurchasingGroup
                        """
                        cur.execute(query_compradores)
                        columns_compradores = [desc[0] for desc in cur.description]
                        df_compradores = pd.DataFrame(cur.fetchall(), columns=columns_compradores)
                        
                        query_po = f"""
                            SELECT
                                "PurchaseOrder",
                                "CreatedByUser",
                                "PurchasingGroup",
                                "PurchasingProcessingStatus",
                                "Supplier",
                                "ZZ1_Aprovador1_PDH",
                                "ZZ1_Aprovador2_PDH",
                                "ZZ1_Aprovador3_PDH",
                                "ZZ1_Aprovador4_PDH",
                                "PurgReleaseTimeTotalAmount",
                                "ExchangeRate",
                                "PurchaseOrderDate"
                            FROM {SCHEMA2}.I_PurchaseOrderAPI01
                        """
                        cur.execute(query_po)
                        columns_po = [desc[0] for desc in cur.description]
                        df_aprov = pd.DataFrame(cur.fetchall(), columns=columns_po)
                        
                    
    except Exception as e:
          print(f'Identificamos a falha: {e}')

    return df_itens, df_entrega, df_compradores, df_aprov

def df_teradata():
    df_itens, df_entrega, df_compradores, df_aprov = base_teradata()

    # --- filtros e normalizações de aprovadores ---
    df_aprov["PurchasingProcessingStatus"] = df_aprov["PurchasingProcessingStatus"].astype(str).str.strip()
    df_aprov = df_aprov[~df_aprov["PurchasingProcessingStatus"].isin(["08","02"])]
    df_aprov["PurchasingGroup"] = df_aprov["PurchasingGroup"].astype(str).str.strip()
    df_aprov = df_aprov.merge(
        df_compradores[["PurchasingGroup","PurchasingGroupName"]],
        on="PurchasingGroup", how="left"
    )

    # --- chaves PO+item (em ambas as bases) ---
    df_itens["PurchaseOrderItem"]   = df_itens["PurchaseOrderItem"].astype(int).astype(str)
    df_entrega["PurchaseOrderItem"] = df_entrega["PurchaseOrderItem"].astype(int).astype(str)

    df_itens["chave"]   = df_itens["PurchaseOrder"]   + df_itens["PurchaseOrderItem"]
    df_entrega["chave"] = df_entrega["PurchaseOrder"] + df_entrega["PurchaseOrderItem"]

    # --- pegar a data MAIS RECENTE de entrega por chave ---
    df_entrega["ScheduleLineDeliveryDate"] = pd.to_datetime(
        df_entrega["ScheduleLineDeliveryDate"], errors="coerce"
    )

    # agrega por chave pegando o máximo (data mais recente), ignorando NaT automaticamente
    df_entrega_max = (
        df_entrega
        .dropna(subset=["ScheduleLineDeliveryDate"])
        .groupby("chave", as_index=False)["ScheduleLineDeliveryDate"]
        .max()
    )

    # --- merge dos itens com a data mais recente de entrega ---
    df_itens = df_itens.merge(df_entrega_max, on="chave", how="left")

    # --- cálculos numéricos ---
    df_itens["OrderQuantity"]  = pd.to_numeric(df_itens["OrderQuantity"], errors="coerce")
    df_itens["NetPriceAmount"] = pd.to_numeric(df_itens["NetPriceAmount"], errors="coerce")
    df_itens["LineValue"] = (df_itens["OrderQuantity"] * df_itens["NetPriceAmount"]).astype(float)
    df_itens["LineValue2"] = df_itens["NetAmount"]

    # --- info de compradores ---
    df_itens = df_itens.merge(
        df_aprov[["PurchaseOrder","PurchasingGroupName"]],
        on="PurchaseOrder", how="left"
    )
    df_itens["RequisitionerName"] = df_itens["RequisitionerName"].astype(str) + " / " + df_itens["PurchasingGroupName"].astype(str)

    # --- colunas finais ---
    mapa_colunas = [
        "PurchaseOrder","PurchaseOrderItem","chave","RequisitionerName","Material",
        "PurchaseOrderItemText","OrderQuantity","DocumentCurrency","NetPriceAmount",
        "LineValue","LineValue2","ScheduleLineDeliveryDate"
    ]
    
    df_itens = df_itens[mapa_colunas]

    return df_itens
