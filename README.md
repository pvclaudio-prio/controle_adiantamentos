# 📊 Aplicativo de Adiantamentos

Sistema corporativo desenvolvido em **Streamlit** para gestão, análise e auditoria de **adiantamentos corporativos**, com integração a banco de dados **Teradata**, aplicação de regras de auditoria, agentes de IA e dashboards interativos.

---

## 🚀 Funcionalidades Principais

### 🔐 Autenticação
- Login seguro utilizando `st.secrets` para controle de acesso.
- Perfis de usuários definidos por credenciais armazenadas em ambiente seguro.

### 🗂️ Estrutura de Abas
O aplicativo é organizado em múltiplas abas, cada uma com funções específicas:

1. **📥 Upload & Carga de Dados**
   - Leitura de bases do **Teradata** em tempo real.
   - Upload de arquivos complementares (CSV/Excel).
   - Limpeza e normalização automática dos dados.

2. **🔎 P1 – Expurgos**
   - Identificação e exclusão de documentos inválidos ou duplicados.
   - Tratamento automático da base para análises consistentes.

3. **📑 P2 – Pagamentos**
   - Consolidação dos documentos de pagamento.
   - Identificação de pagamentos abertos, compensados e estornados.
   - Regras específicas para empresas (ex.: 0600).

4. **📊 P3 – Conciliação**
   - Comparação entre **pedidos de adiantamentos** e **registros contábeis**.
   - Aplicação de filtros inteligentes por status e empresa.
   - Identificação de divergências e inconsistências.

5. **📈 P4 – Análise de Itens**
   - Importação da base de itens do Teradata.
   - Enriquecimento com regras de auditoria.
   - Criação de indicadores de risco e bandeiras 🚩 (Red Flags).

6. **🧮 P5 – Baixas e Compensações**
   - Agrupamento de lançamentos por chave de compensação.
   - Identificação de documentos pagos, compensados e pendentes.

7. **🤖 Agente de IA**
   - Integração com **OpenAI GPT-4o** para revisão dos resultados.
   - Geração de colunas automáticas:
     - `Revisão` (Sim/Não)
     - `Motivo` (justificativa objetiva e precisa).
   - Estimativa de tokens e custo antes da execução.

8. **📉 Dashboards**
   - Visualizações interativas em tempo real.
   - Análise de adiantamentos por empresa, status e período.
   - Gráficos de valores pagos, compensados e red flags.

9. **⬇️ Exportação**
   - Download das bases processadas em **Excel**.
   - Relatórios executivos em **Word** para auditoria e gestão.

---

## 🛠️ Tecnologias Utilizadas
- [Streamlit](https://streamlit.io/) – Interface interativa.
- [Pandas](https://pandas.pydata.org/) – Manipulação de dados.
- [NumPy](https://numpy.org/) – Operações numéricas.
- [OpenAI API](https://platform.openai.com/) – Geração de insights com IA.
- [Teradata](https://www.teradata.com/) – Banco de dados corporativo.
- [Plotly](https://plotly.com/python/) – Dashboards e visualizações.



