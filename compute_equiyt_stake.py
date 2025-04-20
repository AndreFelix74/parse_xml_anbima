#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:31:03 2025

@author: andrefelix
"""


import os
import inspect
import networkx as nx
import pandas as pd
import util as utl
import data_access as dta


DEBUG = False


def print_debug(msg):
    """
    Prints the given message if debugging is enabled.

    Parameters:
    msg (str): The message to be printed.

    This function only prints the message when the global DEBUG variable is set to True.
    If DEBUG is False, the function does nothing.

    Example:
    >>> DEBUG = True
    >>> print_debug("This is a debug message.")
    This is a debug message.
    """
    if DEBUG:
        print(msg)


def calculate_funds_returns(df_funds):
    """
    Calculate the return (rentabilidade) of each fund based on
    the variation in puposicao (price per quota) over time.

    Args:
        df_funds (pd.DataFrame): DataFrame containing price data with columns:
                           'cnpjfundo', 'dtposicao', 'puposicao'.

    Returns:
        pd.DataFrame: A copy of the input DataFrame with an added 'rentabilidade' column,
                      which represents the return between subsequent positions for each fund.
    """
    required_columns = ['cnpjfundo', 'dtposicao', 'puposicao']

    validate_required_columns(df_funds, required_columns)

    returns = df_funds[df_funds['cnpjfundo'].notnull()][required_columns].copy()
    returns.sort_values(by=['cnpjfundo', 'dtposicao'], inplace=True)
    returns['rentabilidade'] = returns.groupby('cnpjfundo')['puposicao'].pct_change()

    return returns


def compute_direct_composition_by_patiliq(investor, group_keys, types_to_exclude):
    """
    Computes the composition of each asset within its portfolio group, based on
    the 'valor_calc' column. The total per portfolio is calculated by grouping on
    ['codcart', 'nome', 'cnpb', 'dtposicao'].

    The result is stored in a new column named 'composicao', representing the 
    percentage share of each asset in the total portfolio.

    Parameters
    ----------
    investor : pandas.DataFrame
        DataFrame containing the calculated asset values per portfolio.
        Must include columns: ['codcart', 'nome', 'cnpb', 'dtposicao', 'valor_calc'].

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
        ~investor['tipo'].isin(types_to_exclude + ['partplanprev'])
    ][group_keys + ['valor_calc', 'tipo']].copy()

    composition['original_index'] = composition.index

    patliq = investor[investor['tipo'] == 'patliq'][group_keys + ['valor_serie']].copy()
    patliq.rename(columns={'valor_serie': 'valor_patliq'}, inplace=True)

    composition = composition.merge(
        patliq,
        on=group_keys,
        how='inner'
    )

    composition['composicao'] = (
        pd.to_numeric(composition['valor_calc'], errors='raise') /
        pd.to_numeric(composition['valor_patliq'], errors='raise')
    )

    composition.set_index('original_index', inplace=True)

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


def get_ordered_funds(df):
    """
    Retorna uma lista de CNPJs de fundos em ordem topológica, com base nas dependências entre eles.
    """
    graph = nx.DiGraph()
    for _, row in df.iterrows():
        investor = row['cnpj']
        invested = row.get('cnpjfundo')
        if pd.notnull(invested):
            graph.add_edge(invested, investor)

    try:
        return list(nx.topological_sort(graph))
    except nx.NetworkXUnfeasible:
        raise ValueError("Ciclo detectado nas dependências entre fundos.")


def harmonize_values(dtfr, harmonization_rules):
    """
    Harmonize values in a DataFrame based on a set of rules.

    This function applies transformations to a specified column (`valor`) in a pandas DataFrame
    using a set of harmonization rules defined in a dictionary. Each rule specifies filters to
    select rows and a formula to compute the new value for the `valor` column.

    Parameters:
    ----------
    dtfr : pandas.DataFrame
        The input DataFrame containing data to be harmonized.
        Must include columns referenced in the filters and formulas.

    harmonization_rules : dict
        A dictionary where each key represents a harmonization rule name
        and each value is a dictionary containing:
        - "filters": A list of dictionaries, where each dictionary specifies:
            - "column" (str): The name of the column to filter.
            - "value" (str or any): The value to match for filtering.
        - "formula": A string representing a formula to evaluate,
          a list of columns to sum, or a constant value.

    Returns:
    -------
    pandas.DataFrame
        The DataFrame with the `valor` column harmonized according to the rules.

    Formula Handling:
    -----------------
    - If `formula` is a string: It is evaluated as a pandas expression using the `eval` function.
    - If `formula` is a list: The specified columns are summed across rows.
    - If `formula` is a constant: The value is directly assigned to the `valor` column.

    Notes:
    ------
    - Rows not matching any rule will retain `None` or their original value in the `valor` column.
    - Warnings are printed if any filter references columns missing from the DataFrame.

    Example:
    --------
    >>> import pandas as pd
    >>> data = {'tipo': ['caixa', 'cotas', 'caixa'], 'saldo': [100, 200, 300]}
    >>> df = pd.DataFrame(data)
    >>> rules = {
    ...     "CAIXA": {"filters": [{"column": "tipo", "value": "caixa"}], "formula": "saldo * 1.1"},
    ...     "COTAS": {"filters": [{"column": "tipo", "value": "cotas"}], "formula": "saldo * 0.9"}
    ... }
    >>> harmonized_df = harmonize_values(df, rules)
    >>> print(harmonized_df)
         tipo  saldo  valor
    0   caixa    100  110.0
    1   cotas    200  180.0
    2   caixa    300  330.0
    """
    dtfr['valor_calc'] = None

    for key, value in harmonization_rules.items():
        print_debug(f"{key} harmonization rule")
        filters = value["filters"]
        formula = value["formula"]

        filter_columns = [filter_item['column'] for filter_item in filters]

        validate_required_columns(dtfr, filter_columns)

        mask = pd.Series(True, index=dtfr.index)
        for filter_item in filters:
            column, filter_value = filter_item['column'], filter_item['value']
            mask &= (dtfr[column] == filter_value)

        if sum(mask) == 0:
            continue

        if isinstance(formula, str):
            formula_expr = formula
            dtfr.loc[mask, 'valor_calc'] = dtfr.loc[mask].eval(formula_expr)
        elif isinstance(formula, list):
            dtfr.loc[mask, 'valor_calc'] = dtfr.loc[mask, formula].sum(axis=1)
        else:
            dtfr.loc[mask, 'valor_calc'] = formula


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
    types_series = [key for key, value in header_daily_values.items() if value.get('serie', True)]

    dtypes = dta.read("fundos_metadata")
    harmonization_rules = dta.read('harmonization_values_rules')

    funds = pd.read_excel(f"{xlsx_destination_path}fundos_raw.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(funds, funds)
    funds.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    harmonize_values(funds, harmonization_rules)

    funds['valor_serie'] = funds['valor'].where(funds['tipo'].isin(types_series), 0)
    funds['valor_calc'] = funds['valor_calc'].where(~funds['tipo'].isin(types_series), 0)

    composition = compute_direct_composition_by_patiliq(funds, ['cnpj'], types_series)
    funds.loc[composition.index, 'composicao'] = composition['composicao']

    dtypes = dta.read(f"carteiras_metadata")

    portfolios = pd.read_excel(f"{xlsx_destination_path}carteiras_raw.xlsx",
                               dtype=dtypes)

    equity_stake = compute_equity_stake(portfolios, funds)
    portfolios.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    harmonize_values(portfolios, harmonization_rules)

    portfolios['valor_serie'] = portfolios['valor'].where(portfolios['tipo'].isin(types_series), 0)
    portfolios['valor_calc'] = portfolios['valor_calc'].where(~portfolios['tipo'].isin(types_series), 0)

    composition = compute_direct_composition_by_patiliq(portfolios, ['codcart', 'nome', 'cnpb'], types_series)
    portfolios.loc[composition.index, 'composicao'] = composition['composicao']

    allocated_partplanprev = explode_partplanprev_and_allocate(portfolios, keys_not_allocated)

    portfolios['flag_rateio'] = portfolios.index.isin(allocated_partplanprev['original_index'].unique()).astype(int)

    portfolios = pd.concat([
        portfolios,
        allocated_partplanprev
    ], ignore_index=True)

    portfolios['valor_calc'] = portfolios['valor_calc'].where(portfolios['flag_rateio'] != 1, 0)

    funds_returns = calculate_funds_returns(funds)

    portfolios = portfolios.merge(
        funds_returns[['cnpjfundo', 'dtposicao', 'rentabilidade']],
        on=['cnpjfundo', 'dtposicao'],
        how='left'
    )

    funds = funds.merge(
        funds_returns[['cnpjfundo', 'dtposicao', 'rentabilidade']],
        on=['cnpjfundo', 'dtposicao'],
        how='left'
    )

    funds.to_excel(f"{xlsx_destination_path}fundos.xlsx", index=False)
    portfolios.to_excel(f"{xlsx_destination_path}/carteiras.xlsx", index=False)


if __name__ == "__main__":
    main()
