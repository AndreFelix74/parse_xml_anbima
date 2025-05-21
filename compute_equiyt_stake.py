#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 15 10:42:26 2025

@author: andrefelix
"""


import os
import inspect
import pandas as pd
import util as utl
import data_access as dta
import file_handler as fhdl


def validate_required_columns(df: pd.DataFrame, required_columns: list):
    """
    Validates that all required columns are present in the given DataFrame.
    Automatically identifies the name of the calling function to include in error messages.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        required_columns (list): A list of column names that must be present.

    Raises:
        ValueError: If one or more required columns are missing.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        caller_name = inspect.stack()[1].function
        raise ValueError(f"[{caller_name}] Missing required columns: {', '.join(missing_columns)}")


def compute_composition(investor, group_keys, types_to_exclude):
    """
    Computes the composition of each asset within its portfolio group, based on
    the 'valor_calc' column. The total per portfolio is calculated by grouping on
    group_keys.

    The result is stored in a new column named 'composicao', representing the
    percentage share of each asset in the total portfolio.

    Parameters
    ----------
    investor : pandas.DataFrame
        DataFrame containing the calculated asset values per portfolio.

    Raises
    ------
    ValueError
        If any required columns are missing.
    """
    if 'dtposicao' not in group_keys:
        group_keys = group_keys + ['dtposicao']

    required_columns = group_keys + ['valor_calc', 'tipo']
    validate_required_columns(investor, required_columns)

    composition = investor[
        (~investor['tipo'].isin(types_to_exclude + ['partplanprev'])) &
        (investor['valor_calc'] != 0)
    ][group_keys + ['valor_calc']].copy()

    composition['total_invest'] = (
        composition.groupby(group_keys)['valor_calc']
        .transform('sum')
    )

    composition['composicao'] = (
        pd.to_numeric(composition['valor_calc'], errors='raise') /
        pd.to_numeric(composition['total_invest'], errors='raise')
    )

    return composition


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

    validate_required_columns(df_investor, columns)

    cotas = df_investor[df_investor['cnpjfundo'].notnull()][columns].copy()

    missing_cotas = cotas[~cotas['cnpjfundo'].isin(df_invested['cnpj'])]

    if len(missing_cotas) != 0:
        print(f"cnpjfundo nao encontrado:\n{missing_cotas['cnpjfundo'].unique()}")

    cotas['original_index'] = cotas.index

    columns_invested = ['cnpj', 'valor', 'dtposicao']

    equity_stake = cotas.merge(
        df_invested[df_invested['tipo'] == 'quantidade'][columns_invested],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner'
    )

    equity_stake.set_index('original_index', inplace=True)

    equity_stake['equity_stake'] = equity_stake['qtdisponivel'] / equity_stake['valor']

    return equity_stake


def main():
    """
    Main function for processing fund and portfolio data:
    - Reads configuration settings.
    - Loads raw fund and portfolio data from Excel files.
    - Computes equity stake
    - Saves processed data back to Excel files.
    """
    config = utl.load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"
    file_ext = config['Paths'].get('destination_file_extension', 'xlsx')

    header_daily_values = dta.read('header_daily_values')
    types_series = [key for key, value in header_daily_values.items() if value.get('serie', True)]

    entities = [
        {
            'name': 'fundos',
            'group_keys': ['cnpj']
        },
        {
            'name': 'carteiras',
            'group_keys': ['cnpjcpf', 'codcart', 'dtposicao', 'nome', 'cnpb']
        }
    ]

    for entity_cfg in entities:
        entity_name = entity_cfg['name']
        group_keys = entity_cfg['group_keys']

        dtypes = dta.read(f"{entity_name}_metadata")

        file_name = f"{xlsx_destination_path}{entity_name}_enriched"
        entity = fhdl.load_df(file_name, file_ext, dtypes)

        if entity_name == 'fundos':
            invested = entity.copy()

        equity_stake = compute_equity_stake(entity, invested)

        entity.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

        composition = compute_composition(entity, group_keys, types_series)
        entity.loc[composition.index, 'composicao'] = composition['composicao']

        file_name = f"{xlsx_destination_path}{entity_name}"
        fhdl.save_df(entity, file_name, file_ext)


if __name__ == "__main__":
    main()
