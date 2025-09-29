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
def carga_bases_p5():
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_inpago, df_rze_0600, df_baixas = filtros_iniciais()
    df_itens = df_teradata()
    df_p3_compensada, df_p3_aberta = tratar_bases_p3()
    return df_pm, df_in, df_rze, df_0600, df_expurgo, df_itens, df_p3_compensada, df_p3_aberta, df_rze_0600, df_baixas

def tratar_bases_p5():
    df_pm, df_in, df_rze, df_0600, df_expurgo, df_itens, df_p3_compensada, df_p3_aberta, df_rze_0600, df_baixas = carga_bases_p5()
    # mantém por segurança no reprocessamento
    df_baixas.rename(columns={"PO Impactada": "PO"}, inplace=True)
    return df_baixas

def layout_p5():
    # ==== AJUSTE: separar colunas textuais e numéricas ====
    EDITABLE_TEXT_COLS = ["PO", "Forma baixa", "N° Título SAP", "Moeda"]
    EDITABLE_NUM_COLS  = ["Valor título", "Valor Utilizado"]
    EDITABLE_COLS      = EDITABLE_TEXT_COLS + EDITABLE_NUM_COLS
    STRING_COLS        = EDITABLE_TEXT_COLS

    NOME_BASE = "df_p5_ajustado.csv"

    @st.cache_data(show_spinner=False)
    def carregar_base(nome_base: str, sep=","):
        df = ler_df_csv_do_drive(nome_base, sep=sep)
        # ==== AJUSTE: normaliza cabeçalhos e migra 'PO Impactada' -> 'PO' ====
        df.columns = df.columns.astype(str).str.strip()
        if "PO" not in df.columns and "PO Impactada" in df.columns:
            df = df.rename(columns={"PO Impactada": "PO"})
        return df

    def salvar_base_no_drive(df: pd.DataFrame, nome_base: str, sobrescrever=True, versionar_timestamp=False):
        salvar_df_csv_no_drive(
            df,
            nome_base=nome_base,
            sobrescrever=sobrescrever,
            versionar_timestamp=versionar_timestamp
        )

    def normalizar_tipos_para_modelo(df: pd.DataFrame) -> pd.DataFrame:
        """Garante tipos corretos para a base persistente (sem formatar datas como string)."""
        df = df.copy()
        # ==== AJUSTE: textos como string, números como numérico ====
        for col in EDITABLE_TEXT_COLS:
            if col in df.columns:
                df[col] = (
                    df[col].astype("string")
                          .str.strip()
                          .replace({"": pd.NA})
                )
        for col in EDITABLE_NUM_COLS:
            if col in df.columns:
                df[col] = df[col].astype(str)

        return df

    def formatar_para_exibicao(df: pd.DataFrame) -> pd.DataFrame:
        """Cria uma cópia apenas para exibição no data_editor (datas formatadas e 'n/i')."""
        view = df.copy()

        # ==== AJUSTE: placeholders apenas para TEXTOS; checagem de existência ====
        for col in EDITABLE_TEXT_COLS:
            if col in view.columns:
                view[col] = view[col].fillna("n/i")

        for col in STRING_COLS:
            if col in view.columns:
                view[col] = view[col].astype(str)

        return view

    def limpar_dados_pos_edicao(df_view_editado: pd.DataFrame, df_base_original: pd.DataFrame) -> pd.DataFrame:
        """
        Recebe o DF retornado do data_editor (formatado pra visual) e reconcilia com a base original,
        preservando tipos e limpando placeholders.
        """
        df = df_view_editado.copy()

        # ==== AJUSTE: remover 'n/i' apenas dos TEXTOS e manter numéricos coerentes ====
        for col in EDITABLE_TEXT_COLS:
            if col in df.columns:
                df[col] = (
                    df[col].replace({"n/i": pd.NA})
                           .astype("string")
                           .str.strip()
                           .replace({"": pd.NA})
                )
        for col in EDITABLE_NUM_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Garantir tipos finais
        df = normalizar_tipos_para_modelo(df)
        return df

    def hash_df_basico(df: pd.DataFrame) -> str:
        """Hash simples para evitar salvar se nada mudou."""
        return pd.util.hash_pandas_object(df.fillna(""), index=True).sum().astype(str)

    # -----------------------------
    # Reprocessamento opcional
    # -----------------------------
    if st.button("🔄 Reprocessar?"):
        with st.spinner("Reprocessando..."):
            df_p5 = tratar_bases_p5()
            df_p5 = normalizar_tipos_para_modelo(df_p5)
            salvar_base_no_drive(df_p5, NOME_BASE, sobrescrever=True, versionar_timestamp=False)
            st.success("Base reprocessada e salva.")
            st.cache_data.clear()  # limpar cache para recarregar

    # -----------------------------
    # Carregar base e preparar exibição
    # -----------------------------
    df_p5_base = carregar_base(NOME_BASE, sep=",")
    df_p5_base = normalizar_tipos_para_modelo(df_p5_base)
    df_view = formatar_para_exibicao(df_p5_base)

    # Guardar hash original na sessão
    if "hash_original" not in st.session_state:
        st.session_state.hash_original = hash_df_basico(df_p5_base)

    # -----------------------------
    # Edição em formulário (evita salvar a cada rerun)
    # -----------------------------
    with st.form("form_edicao_p5"):
        df_p5_editado_view = st.data_editor(
            df_view,
            column_config={
                "PO": st.column_config.TextColumn("PO", width="small"),
                "Forma baixa": st.column_config.TextColumn("Forma baixa", width="small"),
                "N° Título SAP": st.column_config.TextColumn("N° Título SAP", width="small"),
                "Valor título": st.column_config.NumberColumn("Valor título", step=0.01),
                "Valor Utilizado": st.column_config.NumberColumn("Valor Utilizado", step=0.01),
                "Moeda": st.column_config.TextColumn("Moeda", width="small"),
            },
            disabled=[c for c in df_view.columns if c not in EDITABLE_COLS],
            hide_index=True,
            num_rows="dynamic",  
            use_container_width=True,
            height=min(600, 44 + 35 * min(15, len(df_view))),
            key="editor_p5",
        )
    
        col_a, col_b = st.columns([1, 2])
        salvar_click  = col_a.form_submit_button("💾 Salvar alterações", use_container_width=True)
        baixar_click  = col_b.form_submit_button("⬇️ Baixar base (.xlsx)", use_container_width=True)
    
    st.write(f"A base possui **{df_view.shape[0]}** linhas e **{df_view.shape[1]}** colunas.")

    # -----------------------------
    # Ações do formulário
    # -----------------------------
    if baixar_click:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_p5_editado_view.to_excel(writer, index=False, sheet_name="Dados")
        buffer.seek(0)

        # ==== AJUSTE: usar getvalue() para compatibilidade ====
        st.download_button(
            label="⬇️ Baixar base (.xlsx)",
            data=buffer.getvalue(),
            file_name=f"dados_adiantamentos_{pd.Timestamp.today().strftime('%Y-%m-%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
            key="download_base_xlsx",
        )

    if salvar_click:
        with st.spinner("Validando e salvando..."):
            # Reconcilia view -> base
            df_p5_ajustado = limpar_dados_pos_edicao(df_p5_editado_view, df_p5_base)
            novo_hash = hash_df_basico(df_p5_ajustado)

            if novo_hash == st.session_state.hash_original:
                st.info("Nenhuma alteração detectada. Nada foi salvo.")
            else:
                # salvar_base_no_drive(df_p2_ajustado, f"backup/df_p2_ajustado_{datetime.now():%Y%m%d_%H%M%S}.csv", sobrescrever=True, versionar_timestamp=False)
                salvar_base_no_drive(df_p5_ajustado, NOME_BASE, sobrescrever=True, versionar_timestamp=False)
                st.session_state.hash_original = novo_hash
                st.success("Alterações salvas no Drive com sucesso.")
                st.cache_data.clear()
