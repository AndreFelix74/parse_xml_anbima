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
        print(f"cnpjfundo nao encontrado: {missing_cotas['cnpjfundo'].unique()}")

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


def explode_partplanprev_and_allocate(portfolios, types_to_exclude):
    """
    Decomposes aggregated allocations of type 'partplanprev' into proportional
    entries based on real underlying assets in the 'portfolios' dataset.

    This function is specific to portfolios that contain entries of type
    'partplanprev', which represent consolidated participation (e.g., of
    beneficiaries or plans). For each aggregated record, it generates new
    rows representing proportional allocations across the actual portfolio
    assets, using the 'percpart' percentage.

    Parameters
    ----------
    portfolios : pandas.DataFrame
        Must include ['percpart', 'valor_calc', 'codcart', 'nome', 'cnpb', 'dtposicao', 'tipo'].

    types_to_exclude : list of str
        A list of non-asset types that should be excluded from the allocation process.
        Typically includes series-like records or auxiliary types.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the newly generated rows, each representing a
        proportional allocation from a 'partplanprev' entry. Includes:
        - 'valor_calc': calculated based on the original percentage.
        - 'flag_rateio': a flag set to 0, indicating generated allocation rows.

    Raises
    ------
    ValueError
        If any required columns are missing from the input DataFrame.

    Notes
    -----
    - The function performs an inner join between 'partplanprev' entries and
      the actual underlying assets of the portfolio to compute proportional values.
    - This process effectively expands the data structure by creating new rows.
    """
    required_columns = ['percpart', 'valor_calc', 'codcart', 'nome', 'cnpb', 'dtposicao', 'tipo']
    validate_required_columns(portfolios, required_columns)

    partplanprev = portfolios[portfolios['tipo'] == 'partplanprev'][
        ['codcart', 'nome', 'percpart', 'cnpb', 'dtposicao']
    ]

    assets_to_allocate = portfolios[
        ~portfolios['tipo'].isin(types_to_exclude + ['partplanprev'])
    ].drop(columns=['cnpb', 'percpart'])

    assets_to_allocate = assets_to_allocate.copy()
    assets_to_allocate['original_index'] = assets_to_allocate.index

    allocated_assets = partplanprev.merge(
        assets_to_allocate.dropna(subset=['valor_calc']),
        on=['codcart', 'nome', 'dtposicao'],
        how='inner'
    )

    allocated_assets['percpart'] = pd.to_numeric(allocated_assets['percpart'], errors='coerce')
    allocated_assets['valor_calc'] = pd.to_numeric(allocated_assets['valor_calc'], errors='coerce')

    allocated_assets['valor_calc'] = (
        allocated_assets['percpart'] * allocated_assets['valor_calc'] / 100.0
    )

    allocated_assets['flag_rateio'] = 0

    return allocated_assets


def main():
    """
    Main function for processing fund and portfolio data:
    - Reads configuration settings.
    - Loads raw fund and portfolio data from Excel files.
    - Computes equity stake and real state equity values.
    - Saves processed data back to Excel files.
    """
    config = utl.load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    header_daily_values = dta.read('header_daily_values')

    keys_not_allocated = [key for key, value in header_daily_values.items() if value.get('serie', False)]

    dtypes = dta.read("fundos_metadata")
    funds = pd.read_excel(f"{xlsx_destination_path}fundos_staged.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(funds, funds)
    funds.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    funds.to_excel(f"{xlsx_destination_path}fundos.xlsx", index=False)

    dtypes = dta.read(f"carteiras_metadata")

    portfolios = pd.read_excel(f"{xlsx_destination_path}carteiras_staged.xlsx",
                               dtype=dtypes)

    equity_stake = compute_equity_stake(portfolios, funds)
    portfolios.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    allocated_partplanprev = explode_partplanprev_and_allocate(portfolios, keys_not_allocated)

    portfolios['flag_rateio'] = portfolios.index.isin(allocated_partplanprev['original_index'].unique()).astype(int)

    portfolios = pd.concat([
        portfolios,
        allocated_partplanprev
    ], ignore_index=True)

    portfolios['valor_calc'] = portfolios['valor_calc'].where(portfolios['flag_rateio'] != 1, 0)

    portfolios.to_excel(f"{xlsx_destination_path}/carteiras.xlsx", index=False)


if __name__ == "__main__":
    main()
