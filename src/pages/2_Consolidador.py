from frontend import (
    PageConfig, Header, OrdersReader, MessageDisplay, ResultDisplay
)

from backend import DataProcessor, FixingTopForecastingFile

import streamlit as st

def main():
    page_config = PageConfig(page_title="Consolidador", layout="wide")
    header = Header(title="Consolidador", subtitle=None)
    header.display_header()
    orders_reader = OrdersReader()
    message_display = MessageDisplay()
    data_processor = DataProcessor()
    result_display = ResultDisplay()

    QUEBRAS = ["SAO PAULO", "RIO - ZONA SUL", "BRASIL_SEM_PRACA"]

    upload_top_forecasting = orders_reader.upload_file("Carregue o arquivo de previsão que você gerou! (`previsao_top.xslx`)")
    upload_adjusted_baseline = orders_reader.upload_file("Carregue o arquivo baseline que você ajustou!")

    if upload_top_forecasting and upload_adjusted_baseline:
        message_display.display_processing_message()

        def log_callback(message):
            message_display.update_processing_message(message)

        df, result, errors = data_processor.process_top_forecasting_file(upload_top_forecasting, log_callback)

        baseline, baseline_result, baseline_errors = data_processor.process_adjusted_baseline(upload_adjusted_baseline, log_callback)

        fixer = FixingTopForecastingFile(df)
        fct_brasil = fixer.process_all()

        baseline_melted = data_processor.melting_baseline_adjusted(baseline)

        base_final_shift = data_processor.process_region_data(baseline_melted, fct_brasil, QUEBRAS, "shift")
        base_final_turno_g = data_processor.process_region_data(baseline_melted, fct_brasil, QUEBRAS, "turno_g")
        base_final_turno_g = base_final_turno_g.rename(columns={"turno_g": "shift"})

        df_final = data_processor.final_validation(base_final_shift, base_final_turno_g)

        result_display.display_top_forecasting_results(df_final, result, errors)
        result_display.display_baseline_adjusted_results(baseline, baseline_result, baseline_errors)

        result_display.display_final_output(result, errors, df=df_final)
    else:
        if not upload_top_forecasting:
            st.warning("Por favor, carregue o arquivo de previsão.")
        if not upload_adjusted_baseline:
            st.warning("Por favor, carregue o arquivo baseline ajustado.")

if __name__ == "__main__":
    main()