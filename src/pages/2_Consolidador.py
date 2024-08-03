from frontend import (
    PageConfig, Header, OrdersReader, MessageDisplay, ResultDisplay
)

from backend import DataProcessor, FixingTopForecastingFile

def main():
    page_config = PageConfig(page_title="Consolidador", layout="wide")
    header = Header(title="Consolidador", subtitle=None)
    header.display_header()
    orders_reader = OrdersReader()
    message_display = MessageDisplay()
    data_processor = DataProcessor()
    result_display = ResultDisplay()

    upload_top_forecasting = orders_reader.upload_file("Carregue o arquivo de previsão que você gerou! (`previsao_top.xslx`)")


    errors = []
    if upload_top_forecasting:
        message_display.display_processing_message()

        def log_callback(message):
             message_display.update_processing_message(message)

        df, result, errors = data_processor.process_top_forecasting_file(upload_top_forecasting, log_callback)

        fixer = FixingTopForecastingFile(df)

        df_final = fixer.process_all()

        result_display.display_top_forecasting_results(df_final, result, errors)

if __name__ == "__main__":
    main()