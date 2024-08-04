import pandas as pd
import numpy as np
from contrato import Orders
from collections import defaultdict
from datetime import datetime, timedelta, date
from typing import Literal
import streamlit as st

class DataProcessor:
    def __init__(self):
        pass

    def _load_file(self, uploaded_file, log_callback):
        if log_callback:
            log_callback("Espere um momento...")
        try:
            df = pd.read_excel(uploaded_file)
            if log_callback:
                log_callback("Arquivo carregado com sucesso.")
            return df, None
        except Exception as e:
            return pd.DataFrame(), f"Erro inesperado ao carregar o arquivo: {str(e)}"

    def process_history_orders(self, uploaded_file, log_callback=None):
        df, error = self._load_file(uploaded_file, log_callback)
        if error:
            return df, error

        errors = []
        extra_cols = set(df.columns) - set(Orders.model_fields.keys())
        if extra_cols:
            return False, f"Colunas extras detectadas no Excel: {', '.join(extra_cols)}"

        if log_callback:
            log_callback("Validando linhas do arquivo...")
        for index, row in df.iterrows():
            try:
                _ = Orders(**row.to_dict())
            except Exception as e:
                errors.append(f"Erro na linha {index + 2}: {e}")

        if log_callback:
            log_callback("Validação concluída.")
        return df, True, errors
    
    def process_top_forecasting_file(self, uploaded_file, log_callback=None):
            df, error = self._load_file(uploaded_file, log_callback)
            if error:
                return df, error

            if df.shape[0] != 22:
                return False, f"O número de linhas no arquivo deve ser 22, mas o arquivo possui {df.shape[0]} linhas."

            return df, True, []
    
    def process_adjusted_baseline(self, uploaded_file, old_final_baseline, log_callback=None):
        df, error = self._load_file(uploaded_file, log_callback)
        if error:
            return df, error

        errors = []

        # Obter as 5 primeiras colunas do DataFrame
        first_five_columns = set(df.columns[:5])
        
        # Obter as colunas do contrato do modelo
        contract_columns = set(Orders.__annotations__.keys())
        
        # Verificar se todas as 5 primeiras colunas estão no contrato
        if not first_five_columns.issubset(contract_columns):
            missing_columns = first_five_columns - contract_columns
            errors.append(f"Colunas das 5 primeiras posições não estão todas no contrato: {', '.join(missing_columns)}")

        if log_callback:
            log_callback("Validação das 5 primeiras colunas concluída.")

        return df, not errors, errors


        
        
    @staticmethod
    def filter_dataframe(df: pd.DataFrame, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
        df['data_entrega'] = pd.to_datetime(df['data_entrega'], format='%Y-%m-%d')
        filtered_df = df[(df['data_entrega'] >= pd.Timestamp(start_date)) & (df['data_entrega'] <= pd.Timestamp(end_date))].reset_index(drop=True)
        return filtered_df
    
    @staticmethod
    def order_data_enricher(df: pd.DataFrame) -> pd.DataFrame:
        df["ano"] = df["data_entrega"].dt.year
        df["dds"] = df["data_entrega"].dt.weekday
        df["mes"] = df["data_entrega"].dt.month
        df["semana"] = df["data_entrega"].dt.isocalendar().week
        df["ano_semana"] = df["ano"] * 100 + df["semana"]
        df = df.sort_values(by=['data_entrega','ano', 'mes', 'semana', 'modal', 'logistic_region', 'shift', 'turno_g'])
        df['qtd_pedido_lw'] = df.groupby(['modal', 'big_region', 'logistic_region', 'shift', 'turno_g'], observed=False)['qtd_pedido'].shift(7)
        df["var_lw"] = (df["qtd_pedido"] / df["qtd_pedido_lw"] - 1).round(4).fillna(0)
        max_week = int(df["ano_semana"].max())
        df = df.query(f"ano_semana < {max_week}").copy()
        df["qtd_pedido_lw"] = df["qtd_pedido_lw"].fillna(0)
        return df

    @staticmethod
    def allowed_squares(end_date: date, df_filtrado):

        df_filtrado['data_entrega'] = pd.to_datetime(df_filtrado['data_entrega']).dt.date

        seis_semanas = timedelta(weeks=6)

        allowed_squares = (
            df_filtrado[
                (df_filtrado["data_entrega"] >= (end_date - seis_semanas)) &
                (df_filtrado["data_entrega"] <= end_date)
            ]
            [["modal", "logistic_region"]].drop_duplicates()
        )

        return allowed_squares
    
    @staticmethod
    def history_three_weeks(df: pd.DataFrame, DATA_MAXIMA) -> pd.DataFrame:
        tres_semanas = timedelta(weeks=3)

        # Garantir que DATA_MAXIMA é um pd.Timestamp
        DATA_MAXIMA = pd.Timestamp(DATA_MAXIMA)

        # Converter a coluna data_entrega para pd.Timestamp se necessário
        if df["data_entrega"].dtype == 'object':
            df["data_entrega"] = pd.to_datetime(df["data_entrega"])

        # Calcular a data mínima como pd.Timestamp
        data_minima = DATA_MAXIMA - tres_semanas

        # Filtrar o DataFrame
        historico_tres_semanas = df[
            (df["data_entrega"] >= data_minima) &
            (df["data_entrega"] <= DATA_MAXIMA)
        ]

        return historico_tres_semanas
    
    @staticmethod
    def calculate_central_tendency(df: pd.DataFrame, cols_to_calc: list, type: Literal["median", "mean"] = "median") -> pd.DataFrame:
    
        lista_medidas = []
        for col in cols_to_calc:
            new_col_name = f"{type}_{col}"
            # Supondo que df_filtrado seja um DataFrame pandas
            data = df.to_dict('records')  # Converte o DataFrame para uma lista de dicionários

            # Preencher valores NaN com 0
            for entry in data:
                entry[col] = 0 if pd.isna(entry[col]) else entry[col]

            # Agrupar os dados
            grouped_data = defaultdict(list)
            for entry in data:
                key = (entry['modal'], entry['big_region'], entry['logistic_region'], entry['shift'], entry['turno_g'], entry["dds"])
                grouped_data[key].append(entry[col])

            # Calcular a mediana para cada combinação
            medida = {}
            for key, values in grouped_data.items():
                if type == "median":
                    central_tendency = np.median(values)
                    medida[key] = central_tendency
                elif type == "mean":
                    central_tendency = np.mean(values)
                    medida[key] = central_tendency 

            # Criar DataFrame a partir do dicionário de medidas
            df_medidas = pd.DataFrame(
                [{'modal': key[0], 'big_region': key[1], 'logistic_region': key[2], 'shift': key[3], 'turno_g': key[4], 'dds': key[5],new_col_name: central_tendency}
                for key, central_tendency in medida.items()]
            )
            lista_medidas.append(df_medidas)

        if type == "median":
            return lista_medidas
        elif type == "mean":
            return pd.concat(lista_medidas, ignore_index=True)
    
    @staticmethod
    def clip_growth_and_merge(lista_medianas: list) -> pd.DataFrame:
        start_df = []
        for lista in lista_medianas:
            df = pd.DataFrame(lista)
            if "median_var_lw" in df.columns:
                df["median_var_lw"] = np.clip(df["median_var_lw"], -0.1, 1.3)

            start_df.append(df)
        final_df = start_df[0].merge(start_df[1], on=["big_region", "logistic_region","modal" ,"shift", "turno_g", "dds"], how='left')

        final_df["orders"] = (final_df["median_qtd_pedido"] * (1 + final_df["median_var_lw"])).round(0)

        return final_df
    
    @staticmethod
    def adjust_baseline(baseline, media_tres_semanas):
        baseline_comparison = baseline.merge(media_tres_semanas, on=["modal", "big_region", "logistic_region", "shift", "turno_g", "dds"], how='left')

        baseline_comparison["mean_qtd_pedido"] = baseline_comparison["mean_qtd_pedido"].fillna(0)

        baseline_comparison["var"] = abs(np.where(
            baseline_comparison["mean_qtd_pedido"] != 0,
            baseline_comparison["orders"] / baseline_comparison["mean_qtd_pedido"] - 1,
            0
        ))
        baseline_comparison["orders"] = np.where(
            baseline_comparison["var"] > 0.2, # SE A VARIAÇÃO ENTRE MEDIANA E MÉDIA DAS ÚLTIMAS 3 SEMANAS FOR SUPERIOR A 20%, MANTENHA A MÉDIA DAS ÚLTIMAS 3 SEMANAS
            baseline_comparison["mean_qtd_pedido"], baseline_comparison["orders"]
        )

        baseline_comparison["orders"] = baseline_comparison["orders"].round(0).astype(int)

        return baseline_comparison
    
    @staticmethod
    def create_baseline_forecast(dias_previsao: list, baseline: pd.DataFrame):

        baseline_por_praca = dias_previsao.merge(baseline, on="dds", how='right').sort_values(by="data_entrega").reset_index(drop=True)
        baseline_por_praca["data_entrega"] = baseline_por_praca["data_entrega"].dt.date

        baseline_por_praca = (
            baseline_por_praca
            .pivot_table(
                index=["big_region", "logistic_region", "modal", "shift", "turno_g"],
                columns="data_entrega",
                values="orders",
                aggfunc='sum',
                fill_value=0
            )
            .reset_index()
            .sort_values(by="logistic_region")
        )
        return baseline_por_praca
    
    @staticmethod
    def baseline_output(baseline_por_praca: pd.DataFrame, allowed_squares: pd.DataFrame) -> pd.DataFrame:

        baseline_por_praca = baseline_por_praca.merge(allowed_squares, on=["modal", "logistic_region"], how='inner')

        return baseline_por_praca
    
class FixingTopForecastingFile:
    def __init__(self, uploaded_data):
        self.uploaded_data = uploaded_data


    def list_forecastings_squares(self) -> list:
        nan_rows = self.uploaded_data.index[self.uploaded_data.isna().all(axis=1)].tolist()

        # Dividir o DataFrame com base nas linhas de NaN
        dfs = []
        start_idx = 0

        for idx in nan_rows:
            if idx > start_idx:
                # Adiciona o DataFrame da seção anterior
                dfs.append(self.uploaded_data.iloc[start_idx:idx].reset_index(drop=True))
                start_idx = idx + 1

        # Adiciona o último DataFrame
        dfs.append(self.uploaded_data.iloc[start_idx:].reset_index(drop=True))

        return dfs
    
    def fixing_columns(self, dfs):
        lista_previsoes = ["BRASIL-SEM-PRACA", "BRASIL", "SP", "RIO-ZONA-SUL"]

        if len(lista_previsoes) != len(dfs):
            raise ValueError("A lista de previsões deve ter o mesmo comprimento que a lista de DataFrames")

        # Adicionar a coluna com o literal correspondente e garantir que seja a primeira coluna
        for i, df in enumerate(dfs):
            df.insert(0, 'Tipo', lista_previsoes[i])

    def set_headers(self, dfs):
        # Definir o cabeçalho para o primeiro DataFrame
        dfs[0].columns = dfs[0].iloc[0]  # Define a primeira linha como cabeçalho
        dfs[0] = dfs[0][1:]  # Remove a primeira linha que agora é o cabeçalho
        dfs[0].reset_index(drop=True, inplace=True)  # Resetar o índice

        # Definir o cabeçalho para os DataFrames restantes
        for i in range(1, len(dfs)):
            dfs[i].columns = dfs[i].iloc[1]  # Define a segunda linha como cabeçalho
            dfs[i] = dfs[i][2:]  # Remove as duas primeiras linhas que agora são cabeçalhos
            dfs[i].reset_index(drop=True, inplace=True)  # Resetar o índice

    def transform_dates_to_rows(self, df):
        # Supondo que as colunas que devem ser transformadas começam a partir da coluna 2
        value_vars = df.columns[2:]  # Seleciona todas as colunas a partir da terceira
        df_melted = pd.melt(df, id_vars=df.columns[:2], value_vars=value_vars,
                            var_name='DATA', value_name='ORDERS')
        return df_melted

    def process_dataframe(self, df):
        # Renomear a segunda coluna para 'origem'
        df.rename(columns={df.columns[0]: 'ORIGEM'}, inplace=True)
        
        # Converter a coluna 'DATA' para datetime
        df['DATA'] = pd.to_datetime(df['DATA'])
        
        return df

    def process_all(self):
        # Aplicar a função de fixing_columns
        dfs = self.list_forecastings_squares()
        self.fixing_columns(dfs)

        # Definir os cabeçalhos
        self.set_headers(dfs)

        # Transformar as datas para linhas
        dfs_transformed = [self.transform_dates_to_rows(df) for df in dfs]

        # Processar e renomear colunas
        dfs_processed = [self.process_dataframe(df) for df in dfs_transformed]

        # Concatenar todos os DataFrames em um único DataFrame
        df_final = pd.concat(dfs_processed, ignore_index=True)

        # Converter valores de 'ORDERS' para float
        df_final["ORDERS"] = df_final["ORDERS"].astype(float)

        return df_final



class DateUtils:
    @staticmethod
    def generate_dates_until_end_of_month(data, incluir_proximo_mes=False) -> pd.DataFrame:
        if isinstance(data, date) and not isinstance(data, datetime):
            data = datetime(data.year, data.month, data.day)
        dias = []
        while data.month == (data + timedelta(days=1)).month:
            data += timedelta(days=1)
            dias.append(data.strftime('%Y-%m-%d'))
        if incluir_proximo_mes:
            data = datetime(data.year + (data.month // 12), ((data.month % 12) + 1), 1)
            while data.month == ((data + timedelta(days=1)).month):
                dias.append(data.strftime('%Y-%m-%d'))
                data += timedelta(days=1)
            dias.append(data.strftime('%Y-%m-%d'))
        df = pd.DataFrame(dias, columns=['data_entrega'])
        df["data_entrega"] = pd.to_datetime(df["data_entrega"], format="%Y-%m-%d")
        df["dds"] = df["data_entrega"].dt.weekday
        return df
    
