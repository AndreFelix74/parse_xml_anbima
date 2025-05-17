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


def integrate_allocated_partplanprev(entity, allocated_partplanprev):
    """
    Integrates allocated partplanprev rows into the original 'carteiras' DataFrame.

    This includes:
    - Flagging original rows that were used in the allocation.
    - Appending the newly allocated rows.
    - Zeroing out 'valor_calc' for original rows that were split.

    Args:
        entity (pd.DataFrame): Original carteiras DataFrame.
        allocated_partplanprev (pd.DataFrame): Rows generated by
        explode_partplanprev_and_allocate().

    Returns:
        pd.DataFrame: The updated DataFrame with new rows and adjusted flags/values.
    """
    entity['flag_rateio'] = entity.index.isin(
        allocated_partplanprev['original_index'].unique()
    ).astype(int)

    entity = pd.concat([entity, allocated_partplanprev], ignore_index=True)
    entity['valor_calc'] = entity['valor_calc'].where(entity['flag_rateio'] != 1, 0)

    return entity


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

    header_daily_values = dta.read('header_daily_values')
    keys_not_allocated = [
        key
        for key, value in header_daily_values.items()
        if value.get('serie', False)
    ]

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

        entity = pd.read_excel(f"{xlsx_destination_path}{entity_name}_staged.xlsx", dtype=dtypes)

        if entity_name == 'fundos':
            invested = entity.copy()

        equity_stake = compute_equity_stake(entity, invested)

        entity.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

        if entity_name == 'carteiras':
            allocated_partplanprev = explode_partplanprev_and_allocate(entity, keys_not_allocated)
            entity = integrate_allocated_partplanprev(entity, allocated_partplanprev)

        composition = compute_composition(entity, group_keys, types_series)
        entity.loc[composition.index, 'composicao'] = composition['composicao']

        entity.to_excel(f"{xlsx_destination_path}{entity_name}.xlsx", index=False)


if __name__ == "__main__":
    main()
