import streamlit as st
from abas.p5 import layout_p5
from abas.p4 import layout_p4
from abas.p3 import layout_p3
from abas.p2 import layout_p2

st.set_page_config(
    page_title="Controle de Adiantamentos - PRIO",
    page_icon="💸",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.image("PRIO_SEM_POLVO_PRIO_PANTONE_LOGOTIPO_Azul.png", width=200)
st.title("Controle de Adiantamentos")

aba1, aba2, aba3, aba4, aba5 = st.tabs(["Análise", "Dados", "Pend. Miro", "Pgtos em aberto", "Outras baixas"])

with aba1:
    st.markdown("Em construção")
    
with aba2:
    layout_p2()
    
with aba3:
    layout_p3()
    
with aba4:
    layout_p4()

with aba5:
    layout_p5()
