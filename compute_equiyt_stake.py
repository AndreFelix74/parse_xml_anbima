#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:31:03 2025

@author: andrefelix
"""


from configparser import ConfigParser
import pandas as pd


def load_config(config_file):
    """
    Load configuration settings from a specified INI file.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        ConfigParser: A ConfigParser object containing the loaded configuration.
    """
    config = ConfigParser()
    config.read(config_file)

    return config


def compute_equity_stake(df_investor, df_invested):
    """
    Calculate the equity stake of investors based on available quotas and fund values.

    Args:
        df_investor (pd.DataFrame): DataFrame containing investor data with columns
                                    'cnpjfundo', 'qtdisponivel', and 'dtposicao'.
        df_invested (pd.DataFrame): DataFrame containing fund data with columns
                                    'cnpj', 'valor', and 'dtposicao'.

    Returns:
        pd.DataFrame: A DataFrame with the calculated equity stake for each investor.
    """
    columns = ['cnpjfundo', 'qtdisponivel', 'dtposicao']

    cotas = df_investor[df_investor['cnpjfundo'].notnull()][columns]

    missing_cotas = cotas[~cotas['cnpjfundo'].isin(df_invested['cnpj'])]
    
    if len(missing_cotas) != 0:
        print(f"cnpjfundo nao encontrado: {missing_cotas['cnpjfundo'].unique()}" )

    cotas['index_cotas'] = cotas.index

    equity_stake = cotas.merge(
        df_invested[df_invested['tipo'] == "quantidade"][['cnpj', 'valor', 'dtposicao']],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner'
    )

    equity_stake.set_index('index_cotas', inplace=True)

    equity_stake['equity_stake'] = equity_stake['qtdisponivel'] / equity_stake['valor']

    return equity_stake


def compute_equity_real_state(df_investor):
    """
    Compute the real state equity value for investors based on participation percentage
    and book value.

    Args:
        df_investor (pd.DataFrame): DataFrame containing investor data with columns
                                    'percpart' and 'valorcontabil'.

    Returns:
        pd.DataFrame: A DataFrame with the calculated real state equity values.
    """
    columns = ['percpart', 'valorcontabil']

    real_state = df_investor.loc[df_investor['percpart'].notnull(), columns].copy()

    real_state['valor'] = (real_state['percpart'] * real_state['valorcontabil']).round(2)

    return real_state


def get_text_columns_carteiras():
    """
    Get a list of column names that should be interpreted as text for 'carteiras' data.

    Returns:
        list: A list of column names.
    """
    return [
            'isin', 'cnpj', 'nome', 'dtposicao', 'nomeadm', 'cnpjadm', 'nomegestor',
            'cnpjgestor', 'nomecustodiante', 'cnpjcustodiante', 'codanbid',
            'tipofundo', 'nivelrsc', 'tipo', 'codativo', 'cusip', 'dtemissao',
            'dtoperacao', 'dtvencimento', 'depgar', 'tributos', 'indexador',
            'caracteristica', 'classeoperacao', 'idinternoativo', 'compromisso',
            'cnpjemissor', 'coddeb', 'debconv', 'debpartlucro', 'SPE', 'ativo',
            'cnpjcorretora', 'serie', 'hedge', 'tphedge', 'isininstituicao',
            'tpconta', 'txperf', 'vltxperf', 'perctxperf', 'coddesp', 'codprov',
            'credeb', 'dt', 'dtretorno', 'indexadorcomp', 'classecomp', 'cnpjfundo'
            ]


def get_text_columns_fundos():
    """
    Get a list of column names that should be interpreted as text for 'fundos' data.

    Returns:
        list: A list of column names.
    """
    return [
            'isin', 'cnpj', 'nome', 'dtposicao', 'nomeadm', 'cnpjadm', 'nomegestor',
            'cnpjgestor', 'nomecustodiante', 'cnpjcustodiante', 'codanbid',
            'tipofundo', 'nivelrsc', 'tipo', 'codativo', 'cusip', 'dtemissao',
            'dtoperacao', 'dtvencimento', 'depgar', 'tributos', 'indexador',
            'caracteristica', 'classeoperacao', 'idinternoativo', 'compromisso',
            'cnpjemissor', 'coddeb', 'debconv', 'debpartlucro', 'SPE', 'ativo',
            'cnpjcorretora', 'serie', 'hedge', 'tphedge', 'isininstituicao',
            'tpconta', 'txperf', 'vltxperf', 'perctxperf', 'coddesp', 'codprov',
            'credeb', 'dt', 'dtretorno', 'indexadorcomp', 'classecomp', 'cnpjfundo'
            ]


def main():
    """
    Main function for processing fund and portfolio data:
    - Reads configuration settings.
    - Loads raw fund and portfolio data from Excel files.
    - Computes equity stake and real state equity values.
    - Saves processed data back to Excel files.
    """
    config = load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(format_path(xlsx_destination_path))}/"

    fundos = pd.read_excel(f"{xlsx_destination_path}fundos_raw.xlsx",
                           dtype={col: str for col in get_text_columns_fundos()})

    equity_stake = compute_equity_stake(fundos, fundos)
    fundos.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']
    fundos.to_excel(f"{xlsx_destination_path}/fundos.xlsx", index=False)

    carteiras = pd.read_excel(f"{xlsx_destination_path}carteiras_raw.xlsx",
                              dtype={col: str for col in get_text_columns_carteiras()})

    equity_stake = compute_equity_stake(carteiras, fundos)
    carteiras.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    equity_real_state = compute_equity_real_state(carteiras)
    carteiras.loc[equity_real_state.index, 'valor'] = equity_real_state['valor']

    carteiras.to_excel(f"{xlsx_destination_path}/carteiras.xlsx", index=False)


if __name__ == "__main__":
     main()
