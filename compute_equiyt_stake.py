#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:31:03 2025

@author: andrefelix
"""


from configparser import ConfigParser
from collections import defaultdict
import json
import os
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


def format_path(str_path):
    """
    Format a given path to ensure it starts with a proper prefix and ends with a slash.

    Args:
        str_path (str): The input file path.

    Returns:
        str: Formatted path.
    """
    if not str_path.startswith("/") and not str_path.startswith("."):
        str_path = os.path.join("..", "data", str_path)

    if not str_path.endswith("/"):
        str_path += "/"

    return str_path


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
    equity_stake = pd.DataFrame(columns=['equity_stake'])

    columns = ['cotas-cnpjfundo', 'cotas-qtdisponivel', 'header-dtposicao']

    if not all(col in df_investor.columns for col in columns):
        return equity_stake

    cotas = df_investor[df_investor['cotas-cnpjfundo'].notnull()][columns]

    missing_cotas = cotas[~cotas['cotas-cnpjfundo'].isin(df_invested['header-cnpj'])]

    if len(missing_cotas) != 0:
        print(f"cotas-cnpjfundo nao encontrado: {missing_cotas['cotas-cnpjfundo'].unique()}" )

    cotas['index_cotas'] = cotas.index

    columns_invested = ['header-cnpj', 'header-valor', 'header-dtposicao']

    equity_stake = cotas.merge(
        df_invested[df_invested['tipo'] == "header-quantidade"][columns_invested],
        left_on=['cotas-cnpjfundo', 'header-dtposicao'],
        right_on=['header-cnpj', 'header-dtposicao'],
        how='inner'
    )

    equity_stake.set_index('index_cotas', inplace=True)

    equity_stake['equity_stake'] = equity_stake['cotas-qtdisponivel'] / equity_stake['header-valor']

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
    real_state = pd.DataFrame(columns=['valor'])

    columns = ['partplanprev-percpart', 'imoveis-valorcontabil']

    if not all(col in df_investor.columns for col in columns):
        return real_state

    real_state = df_investor.loc[df_investor['partplanprev-percpart'].notnull(), columns].copy()

    real_state['valor'] = real_state['partplanprev-percpart'] * real_state['imoveis-valorcontabil']

    return real_state


def remove_prefix_and_merge_columns_inplace(dataframe):
    """
    Renomeia colunas removendo o prefixo antes de um hífen e mescla colunas duplicadas no mesmo DataFrame.

    Args:
        df (pd.DataFrame): O DataFrame a ser modificado.

    Returns:
        None: As alterações são feitas diretamente no DataFrame original.
    """
    new_columns = [col.split("-", 1)[-1] for col in dataframe.columns]
    column_map = defaultdict(list)
    
    for old_col, new_col in zip(dataframe.columns, new_columns):
        column_map[new_col].append(old_col)

    for new_col, old_cols in column_map.items():
        if len(old_cols) > 1:
            dataframe[new_col] = dataframe[old_cols].bfill(axis=1).iloc[:, 0]
            dataframe.drop(columns=old_cols, inplace=True)
        else:
            dataframe.rename(columns={old_cols[0]: new_col}, inplace=True)


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

    with open(f"{xlsx_destination_path}fundos_metadata.json", "r") as f:
        dtypes = json.load(f)

    fundos = pd.read_excel(f"{xlsx_destination_path}fundos_raw.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(fundos, fundos)
    fundos.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']
    
    with open(f"{xlsx_destination_path}carteiras_metadata.json", "r") as f:
        dtypes = json.load(f)

    carteiras = pd.read_excel(f"{xlsx_destination_path}carteiras_raw.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(carteiras, fundos)
    carteiras.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    equity_real_state = compute_equity_real_state(carteiras)
    carteiras.loc[equity_real_state.index, 'valor'] = equity_real_state['valor']

    remove_prefix_and_merge_columns_inplace(fundos)
    fundos.to_excel(f"{xlsx_destination_path}/fundos.xlsx", index=False)

    remove_prefix_and_merge_columns_inplace(carteiras)
    carteiras.to_excel(f"{xlsx_destination_path}/carteiras.xlsx", index=False)

if __name__ == "__main__":
     main()
