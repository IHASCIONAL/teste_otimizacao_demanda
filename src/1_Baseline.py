from frontend import OrdersReader
from backend import(
    process_excel,
    filter_dataframe,
    feature_engineering,
    calcula_tendencia_central,
    clip_growth_and_merge,
    dias_ate_fim_do_mes,
    baseline_previsao,
    allowed_squares,
    baseline_output,
    history_three_weeks,
    adjust_baseline
)

def main():
    odr = OrdersReader()
    odr.display_header()
    start_date = odr.data_inicial()
    end_date = odr.data_final()
    delivery_date = odr.data_entrega_demanda()
    incluir_mes_seguinte = odr.mes_seguinte()
    uploaded_file = odr.upload_file()

    errors = []
    if uploaded_file:
        odr.display_processing_message()

        def log_callback(message):
             odr.update_processing_message(message)

        df, result, errors = process_excel(uploaded_file, log_callback)

        df_filtered = filter_dataframe(df, start_date, end_date)
        df_filtered = feature_engineering(df_filtered)
        pracas_permitidas = allowed_squares(end_date, df_filtered)

        historico_tres_semanas = history_three_weeks(df_filtered, end_date)
        dias_previsao = dias_ate_fim_do_mes(delivery_date, incluir_mes_seguinte)

        media_tres_semanas = calcula_tendencia_central(historico_tres_semanas, ["qtd_pedido"], "mean")
        lista_medianas = calcula_tendencia_central(df_filtered, ["var_lw", "qtd_pedido"],"median")

        baseline = clip_growth_and_merge(lista_medianas)

        baseline = adjust_baseline(baseline, media_tres_semanas)

        baseline = baseline_previsao(dias_previsao,baseline)

        pracas_permitidas = allowed_squares(end_date, df_filtered)

        final_baseline = baseline_output(baseline, pracas_permitidas)

        odr.display_results(result, errors, df=final_baseline)

    if errors:
            odr.display_wrong_message()


if __name__ == "__main__":
    main()