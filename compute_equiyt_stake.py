#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:31:03 2025

@author: andrefelix
"""


from configparser import ConfigParser
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

    columns = ['cnpjfundo', 'qtdisponivel', 'dtposicao']

    if not all(col in df_investor.columns for col in columns):
        return equity_stake

    cotas = df_investor[df_investor['cnpjfundo'].notnull()][columns]

    missing_cotas = cotas[~cotas['cnpjfundo'].isin(df_invested['cnpj'])]

    if len(missing_cotas) != 0:
        print(f"cnpjfundo nao encontrado: {missing_cotas['cnpjfundo'].unique()}")

    cotas['orinal_index'] = cotas.index

    columns_invested = ['cnpj', 'valor', 'dtposicao']

    equity_stake = cotas.merge(
        df_invested[df_invested['tipo'] == "quantidade"][columns_invested],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner'
    )

    equity_stake.set_index('orinal_index', inplace=True)

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
    required_columns = ['percpart', 'valorcontabil', 'codcart']

    if not all(col in df_investor.columns for col in required_columns):
        return pd.DataFrame(columns=['valor'])

    equity_stake = df_investor[df_investor['percpart'].notna()][['codcart', 'percpart']]
    equity_stake['original_index'] = equity_stake.index

    real_state_equity_book_value = equity_stake.merge(
        df_investor[['codcart', 'valorcontabil']].dropna(subset=['valorcontabil']),
        on='codcart',
        how='inner'
    )

    real_state_equity_book_value['valor'] = (
            (real_state_equity_book_value['percpart'] / 100 )*
            real_state_equity_book_value['valorcontabil']
            )

    real_state_equity_book_value = real_state_equity_book_value.set_index('original_index')

    return real_state_equity_book_value


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

    with open(f"{xlsx_destination_path}fundos_metadata.json", "r") as file:
        dtypes = json.load(file)

    fundos = pd.read_excel(f"{xlsx_destination_path}fundos_raw.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(fundos, fundos)
    fundos.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    fundos.to_excel(f"{xlsx_destination_path}/fundos.xlsx", index=False)

    with open(f"{xlsx_destination_path}carteiras_metadata.json", "r") as file:
        dtypes = json.load(file)

    carteiras = pd.read_excel(f"{xlsx_destination_path}carteiras_raw.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(carteiras, fundos)
    carteiras.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    equity_real_state = compute_equity_real_state(carteiras)
    carteiras.loc[equity_real_state.index, 'valor'] = equity_real_state['valor']

    carteiras.to_excel(f"{xlsx_destination_path}/carteiras.xlsx", index=False)


if __name__ == "__main__":
    main()
