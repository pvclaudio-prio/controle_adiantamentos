from dados.carga import filtros_iniciais
from dados.dados_teradata import df_teradata
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from dados.upload import ler_df_csv_do_drive
from dados.salvar_bases import salvar_df_csv_no_drive
import datetime

@st.cache_data(show_spinner="Carregando dados...")
def carga_bases_p3():
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_rze_0600, df_baixas = filtros_iniciais()
    
    return df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_rze_0600, df_baixas

def tratar_bases_p3():
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_rze_0600, df_baixas = carga_bases_p3()
    
    df_in = df_in[~df_in["Conta"].isin(["1009278","1007279","1011526"])]
    
    mapa_colunas = {
        "Documento de compras":"PO",
        "Conta":"Cód Forn.",
        "Nome Fornecedor":"Fornecedor",
        "Doc.referência":"MIRO",
        "Moeda do documento":"Moeda",
        "Mont.moeda doc.":"Valor"
        }
    df_in.rename(columns=mapa_colunas, inplace=True)
    df_in["MIRO"] = df_in["MIRO"].str[:-4]
    
    df_inpago.rename(columns={"Documento de compras": "PO","Doc.referência":"MIRO"}, inplace=True)
    df_inpago_agrupado = df_inpago.groupby(["PO","MIRO"])[["Pago","Compensado","Estorno"]].agg("sum").reset_index()
    df_inpago_agrupado["chave"] = df_inpago_agrupado["PO"] + df_inpago_agrupado["MIRO"]
    
    df_rze = df_rze[df_rze["Doc.compensação"].isna()]
    df_rze = df_rze[df_rze["Documento de compras"].notna()]
    df_rze_0600 = df_rze_0600[df_rze_0600["Doc.compensação"].isna()]
    df_rze_0600 = df_rze_0600[df_rze_0600["Documento de compras"].notna()]
    
    lista_rze = np.union1d(
    df_rze["Documento de compras"].dropna().astype(str).str.strip(),
    df_rze_0600["Documento de compras"].dropna().astype(str).str.strip()).tolist()

    df_in = df_in[df_in["PO"].isin(lista_rze)]
    df_in["chave"] = df_in["PO"] + df_in["MIRO"]
    
    df_in_agrupado = df_in.groupby(["PO","MIRO"])["Valor"].agg("sum").reset_index()
    df_in_agrupado["chave"] = df_in_agrupado["PO"] + df_in_agrupado["MIRO"]
    df_in_pos = pd.DataFrame({"PO": df_in["PO"].unique()})
    
    df_p3_compensada = df_in_pos.copy()
    df_p3_compensada = df_p3_compensada.merge(df_in[["PO", "Empresa", "Cód Forn.", "Fornecedor", "Referência", "MIRO","chave"]], on="PO", how="left")
    df_p3_compensada = df_p3_compensada.drop_duplicates(["PO", "MIRO"])
    df_p3_compensada = df_p3_compensada.merge(df_in_agrupado[["Valor","chave"]],on = "chave", how = "left")
    df_p3_compensada = df_p3_compensada.merge(df_in[["Moeda","Doc.compensação","chave"]], on="chave", how="left")
    df_p3_compensada = df_p3_compensada.drop_duplicates(["PO", "MIRO","Doc.compensação"])
    df_p3_compensada = df_p3_compensada[df_p3_compensada["Doc.compensação"].astype(str).str[:2].isin(["15","18","19","20","21","48","80"])]
    
    df_p3_compensada = df_p3_compensada.merge(df_inpago_agrupado[["Pago","Compensado","Estorno","chave"]], on="chave", how="left")
    df_p3_compensada.drop(columns="chave", inplace=True)

    df_p3_aberta = df_in[["PO", "Empresa", "Cód Forn.", "Fornecedor", "Referência", "MIRO", "Valor", "Moeda","Doc.compensação",
                          "Vencimento líquido","Data de lançamento"]]
    df_p3_aberta = df_p3_aberta[df_p3_aberta["Doc.compensação"].isna()]
    
    return df_p3_compensada, df_p3_aberta

def layout_p3():
    # carrega suas bases
    df_p3_compensada, df_p3_aberta = tratar_bases_p3()

    # -----------------------------
    # Parâmetros
    # -----------------------------
    NOME_BASE_P3 = "df_p3_ajustado.csv"
    COLS_NUM = ["Valor", "Pago", "Compensado", "Estorno"]
    COLS_STRING = ["PO", "Cód Forn.", "MIRO", "Doc.compensação"]
    COLS_VISUAIS_DATA = []

    # -----------------------------
    # Helpers (Drive)
    # -----------------------------
    @st.cache_data(show_spinner=False)
    def carregar_base_p3(nome_base: str, sep=","):
        return ler_df_csv_do_drive(nome_base, sep=sep)

    def salvar_base_p3(df: pd.DataFrame, nome_base: str, sobrescrever=True, versionar_timestamp=False):
        salvar_df_csv_no_drive(
            df, nome_base=nome_base,
            sobrescrever=sobrescrever, versionar_timestamp=versionar_timestamp
        )

    def tratar_bases_p3_normalizado():
        df = df_p3_compensada.copy()
        for col in COLS_NUM:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        if "linha_id" not in df.columns:
            df.insert(0, "linha_id", np.arange(1, len(df) + 1).astype(int))
        df["check"] = np.where((df["Pago"] + df["Compensado"]).round(2) == df["Valor"].round(2), "Sim", "Não")
        return df

    def normalizar_para_modelo(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        for col in COLS_NUM:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
                df[col] = df[col].astype(float)
    
        if all(c in df.columns for c in ["Pago", "Compensado", "Valor"]):
            cond_pagamento = (df["Pago"] + df["Compensado"]).round(2) == df["Valor"].round(2)
            cond_estorno = "Estorno" in df.columns and (df["Estorno"].round(2) == (df["Valor"] * -1).round(2))
    
            df["check"] = np.where(cond_pagamento | cond_estorno, "Sim", "Não")
        else:
            df["check"] = "Não"
    
        return df


    def formatar_para_exibicao(df: pd.DataFrame) -> pd.DataFrame:
        view = df.copy()
        
        for col in COLS_VISUAIS_DATA:
            if col in view.columns:
                view[col] = pd.to_datetime(view[col], errors="coerce").dt.strftime("%d/%m/%Y").fillna("")
        for col in COLS_STRING:
            view[col] = view[col].astype(str)
            
        return view

    def hash_df(df: pd.DataFrame) -> str:
        return pd.util.hash_pandas_object(df.fillna(""), index=True).sum().astype(str)

    # -----------------------------
    # Reprocessar (opcional)
    # -----------------------------
    if st.button("🔄 Reprocessar dados?"):
        with st.spinner("Reprocessando..."):
            df_p3 = tratar_bases_p3_normalizado()
            df_p3 = normalizar_para_modelo(df_p3)
            salvar_base_p3(df_p3, NOME_BASE_P3, sobrescrever=True, versionar_timestamp=False)
            st.success("Base reprocessada e salva.")
            st.cache_data.clear()

    # -----------------------------
    # Carrega base do Drive (ou cria)
    # -----------------------------
    try:
        df_p3_base = carregar_base_p3(NOME_BASE_P3, sep=",")
    except Exception:
        df_p3_base = tratar_bases_p3_normalizado()
        salvar_base_p3(df_p3_base, NOME_BASE_P3, sobrescrever=True, versionar_timestamp=False)

    df_p3_base = normalizar_para_modelo(df_p3_base)
    df_p3_view = formatar_para_exibicao(df_p3_base)

    if "hash_p3" not in st.session_state:
        st.session_state.hash_p3 = hash_df(df_p3_base)

    # =========================================================
    # 1) SÓ O PRIMEIRO DATAFRAME: editar Pago/Compensado
    # =========================================================
    st.subheader("Editar valores de **Pago** e **Compensado**")
    edit_view = df_p3_view.copy()
    edit_view = edit_view[edit_view["check"]=="Não"]
    
    if "linha_id" not in edit_view.columns:
        edit_view.insert(0, "linha_id", np.arange(1, len(edit_view) + 1).astype(int))

    with st.form("form_editar_pag_comp"):
        df_edit_vals = st.data_editor(
            edit_view,
            hide_index=True,
            use_container_width=True,
            height=500,
            column_config={
                "linha_id": st.column_config.NumberColumn("ID", step=1),
                "Pago": st.column_config.NumberColumn("Pago", min_value=0.0, step=0.01, format="%.2f"),
                "Compensado": st.column_config.NumberColumn("Compensado", min_value=0.0, step=0.01, format="%.2f"),
                "Estorno": st.column_config.NumberColumn("Estorno", min_value=0.0, step=0.01, format="%.2f")
            },
            disabled=[c for c in edit_view.columns if c not in ["linha_id", "Pago", "Compensado","Estorno"]],
            num_rows="fixed",
            key="editor_pag_comp",
        )
        col1, col2 = st.columns([1, 2])
        salvar_vals = col1.form_submit_button("💾 Salvar valores", use_container_width=True)
        baixar_vals = col2.form_submit_button("⬇️ Baixar base (.xlsx)", use_container_width=True)
        
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_edit_vals.to_excel(writer, index=False, sheet_name="Dados")
        
    if baixar_vals:
        st.download_button(
            label="⬇️ Baixar base (.xlsx)",
            data=buffer.getvalue(),
            file_name=f"adiantamentos_compensados_{pd.Timestamp.today().strftime('%Y-%m-%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
        )

    if salvar_vals:
        with st.spinner("Validando e salvando alterações..."):
            upd = df_edit_vals[["linha_id", "Pago", "Compensado"]].copy()
            upd["linha_id"] = pd.to_numeric(upd["linha_id"], errors="coerce").astype("Int64")
            upd["Pago"] = pd.to_numeric(upd["Pago"], errors="coerce").fillna(0.0).clip(lower=0)
            upd["Compensado"] = pd.to_numeric(upd["Compensado"], errors="coerce").fillna(0.0).clip(lower=0)

            df_new = df_p3_base.copy()
            
            if "linha_id" not in df_new.columns:
                df_new.insert(0, "linha_id", np.arange(1, len(df_new) + 1).astype(int))
            df_new = df_new.set_index("linha_id")
            upd = upd.dropna(subset=["linha_id"]).set_index("linha_id")
            for col in ["Pago", "Compensado"]:
                if col in df_new.columns:
                    df_new.loc[upd.index, col] = upd[col]
            df_new = df_new.reset_index()
            df_new = normalizar_para_modelo(df_new)

            new_hash = hash_df(df_new)
            if new_hash == st.session_state.hash_p3:
                st.info("Nenhuma alteração detectada. Nada foi salvo.")
            else:
                salvar_base_p3(df_new, NOME_BASE_P3, sobrescrever=True, versionar_timestamp=False)
                st.session_state.hash_p3 = new_hash
                st.success("Valores atualizados e salvos no Drive.")
                st.cache_data.clear()
    
    st.markdown(f'A base possui **{df_edit_vals.shape[0]}** linhas e **{df_edit_vals.shape[1]}** colunas.')
    
    # =========================================================
    # 2) BOTÃO -> mostra os compensados (check = Sim) para REABRIR
    # =========================================================
    if "show_reabrir" not in st.session_state:
        st.session_state.show_reabrir = False

    def _toggle_reabrir():
        st.session_state.show_reabrir = not st.session_state.show_reabrir
        
    st.markdown("---")
    st.button("Mostrar/ocultar compensados para reabrir", on_click=_toggle_reabrir)

    if st.session_state.show_reabrir:
        st.markdown("Selecione os lançamentos a **reabrir** (zerar `Pago` e `Compensado`).")
        df_comp = df_p3_view[df_p3_view["check"] == "Sim"].copy()
        if "Reabrir?" not in df_comp.columns:
            df_comp.insert(1, "Reabrir?", False)

        with st.form("form_reabertura"):
            df_comp_edit = st.data_editor(
                df_comp,
                hide_index=True,
                use_container_width=True,
                height=420,
                column_config={
                    "Reabrir?": st.column_config.CheckboxColumn("Reabrir?"),
                },
                disabled=[c for c in df_comp.columns if c not in ["Reabrir?"]],
                num_rows="fixed",
                key="editor_reabertura_p3",
            )
            col_a, col_b = st.columns([1, 2])
            aplicar_reabertura = col_a.form_submit_button("↩️ Aplicar Reabertura e Salvar", use_container_width=True)
            baixar_comp = col_b.form_submit_button("⬇️ Baixar base (.xlsx)", use_container_width=True)
        
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_comp_edit.to_excel(writer, index=False, sheet_name="Dados")
            
        if baixar_comp:
            st.download_button(
                label="⬇️ Baixar base (.xlsx)",
                data=buffer.getvalue(),
                file_name=f"adiantamentos_baixados_{pd.Timestamp.today().strftime('%Y-%m-%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=False,
            )

        if aplicar_reabertura:
            with st.spinner("Reabrindo lançamentos selecionados..."):
                ids_reabrir = set(
                    df_comp_edit.loc[df_comp_edit["Reabrir?"] == True, "linha_id"].astype(int).tolist()
                )
                if not ids_reabrir:
                    st.info("Nenhum lançamento marcado para reabertura.")
                else:
                    df_new = df_p3_base.copy()
                    mask = df_new["linha_id"].astype(int).isin(ids_reabrir)
                    for col in ["Pago", "Compensado"]:
                        if col in df_new.columns:
                            df_new.loc[mask, col] = 0.0
                    df_new = normalizar_para_modelo(df_new)

                    new_hash = hash_df(df_new)
                    if new_hash == st.session_state.hash_p3:
                        st.info("Nenhuma alteração efetiva detectada.")
                    else:
                        salvar_base_p3(df_new, NOME_BASE_P3, sobrescrever=True, versionar_timestamp=False)
                        st.session_state.hash_p3 = new_hash
                        st.success(f"Reabertura aplicada em {len(ids_reabrir)} lançamentos e salva no Drive.")
                        st.cache_data.clear()
    
    st.subheader("MIROs em Aberto")
    
    for col in ["Vencimento líquido", "Data de lançamento"]:
        df_p3_aberta[col] = pd.to_datetime(df_p3_aberta[col], errors="coerce").dt.strftime("%d/%m/%Y").fillna("")
    
    st.dataframe(df_p3_aberta)
    st.markdown(f'A base possui **{df_p3_aberta.shape[0]}** linhas e **{df_p3_aberta.shape[1]}** colunas.')
    
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_p3_aberta.to_excel(writer, index=False, sheet_name="Dados")

    col1 = st.columns(1)[0]
    with col1:
        st.download_button(
            label="⬇️ Baixar base (.xlsx)",
            data=buffer.getvalue(),
            file_name=f"adiantamentos_abertos_{pd.Timestamp.today().strftime('%Y-%m-%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
        )
    
