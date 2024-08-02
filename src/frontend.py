import streamlit as st
import pandas as pd
import io

class OrdersReader:

    def __init__(self):
        self.set_page_config()

    def set_page_config(self):
        st.set_page_config(
            page_title="Gerador de Baseline"
        )

    def display_header(self):
        st.title("Gerador de Baseline")

    def data_inicial(self):
        st.write("Selecione o intervalo de datas para filtrar a tabela de pedidos que será utilizada para gerar o baseline!")
        start_date = st.date_input("Data inicial de corte para o baseline", key="start_date")
        return start_date
    
    def data_final(self):
          end_date = st.date_input("Data final de corte para o baseline", key="end_date")
          return end_date
    
    def data_entrega_demanda(self):
          delivery_date = st.date_input("Data em que a demanda será entregue", key="delivery_date")
          return delivery_date

    def mes_seguinte(self):
        incluir_mes_seguinte = st.checkbox("Incluir mês seguinte na previsão?", key="incluir_mes_seguinte")
        return incluir_mes_seguinte
    
    def upload_file(self):
        return st.file_uploader("Carregue o arquivo excel com o histórico de Pedidos aqui!", type=["xlsx"])
    
    def display_processing_message(self):
        self.processing_placeholder = st.empty()
        with self.processing_placeholder:
            with st.spinner("Processando o arquivo..."):
                pass
    
    def update_processing_message(self, message):
        if hasattr(self, 'processing_placeholder'):
            self.processing_placeholder.text(message)
        else:
            st.toast(message)

    def display_wrong_message(self):
        st.error("Houve um problema durante o processamento. Verifique os erros.")


    def display_results(self, result, errors, df):
        if errors:
            for error in errors:
                st.error(f"Erro na validação: {error}")
        else:
            st.success("As colunas do arquivo Excel estão corretas!")
            st.success("Aqui está o seu baseline!")

            buffer = io.BytesIO()

                # Salvar o DataFrame no buffer como Excel
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')

            # Definir o ponteiro do buffer para o início
            buffer.seek(0)

            # Adicionar um botão para download
            st.download_button(
                label="Baixar como Excel",
                data=buffer,
                file_name="baseline.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )