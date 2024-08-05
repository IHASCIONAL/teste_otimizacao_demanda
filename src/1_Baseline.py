from frontend import (PageConfig,
                    Header,
                    OrdersReader,
                    DateInputs,
                    MessageDisplay,
                    ResultDisplay
                    )

from backend import DataProcessor, DateUtils



def main():

    page_config = PageConfig()
    header = Header()
    header.display_header()
    orders_reader = OrdersReader()
    start_date = DateInputs.data_inicial()
    end_date = DateInputs.data_final()
    delivery_date = DateInputs.data_entrega_demanda()
    incluir_mes_seguinte = DateInputs.mes_seguinte()
    message_display = MessageDisplay()
    result_display = ResultDisplay()
    data_processor = DataProcessor()
    date_utils = DateUtils()
 
    
    upload_orders_history = orders_reader.upload_file("Carregue o arquivo excel com o histórico de Pedidos aqui! (`historico_pedidos.xlsx`)")


    errors = []
    if upload_orders_history:
        message_display.display_processing_message()

        def log_callback(message):
             message_display.update_processing_message(message)

        df, result, errors = data_processor.process_history_orders(upload_orders_history, log_callback)

        df_filtered = data_processor.filter_dataframe(df, start_date, end_date)
        df_filtered = data_processor.order_data_enricher(df_filtered)
        pracas_permitidas = data_processor.allowed_squares(end_date, df_filtered)

        dias_previsao = date_utils.generate_dates_until_end_of_month(delivery_date, incluir_mes_seguinte)

        lista_medianas = data_processor.calculate_central_tendency(df_filtered, ["var_lw", "qtd_pedido"],"median")

        baseline = data_processor.clip_growth_and_merge(lista_medianas)

        baseline = data_processor.create_baseline_forecast(dias_previsao,baseline)

        final_baseline = data_processor.baseline_output(baseline, pracas_permitidas)

        result_display.display_baseline_results(result, errors, df=final_baseline)

    if errors:
            message_display.display_wrong_message()


if __name__ == "__main__":
    main()