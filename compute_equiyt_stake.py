#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:31:03 2025

@author: andrefelix
"""


import os
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


def compute_proportional_allocation(df_investor, types_to_exclude):
    """
    Compute the real state equity value for investors based on participation percentage
    and book value.

    Args:
        df_investor (pd.DataFrame): DataFrame containing investor data with columns
                                    'percpart' and 'valor_calc'.

    Returns:
        pd.DataFrame: A DataFrame with the calculated real state equity values.
    """
    required_columns = ['percpart', 'valor_calc', 'codcart', 'nome']

    if not all(col in df_investor.columns for col in required_columns):
        raise ValueError(f"""Error: required columns missing: {', '.join(required_columns)}""")

    allocation = df_investor[df_investor['tipo'] == 'partplanprev'].drop(columns=['valor_calc'])

    invstr_filtrd = df_investor[~df_investor['tipo'].isin(types_to_exclude + ['partplanprev'])]
    invstr_filtrd.loc[:, 'original_index'] = invstr_filtrd.index

    invstr_filtrd.loc[:, ['new_tipo']] = invstr_filtrd['tipo']

    columns_filtrd = ['codcart', 'nome', 'valor_calc', 'new_tipo', 'original_index']
    allocation_value = allocation.merge(
        invstr_filtrd[columns_filtrd].dropna(subset=['valor_calc']),
        on=['codcart', 'nome'],
        how='inner'
    )

    allocation_value['percpart'] = pd.to_numeric(allocation_value['percpart'], errors='coerce')
    allocation_value['valor_calc'] = pd.to_numeric(allocation_value['valor_calc'], errors='coerce')

    allocation_value['valor_calc'] = (
        allocation_value['percpart'] *
        allocation_value['valor_calc'] / 100.0
        )

    allocation_value['tipo'] = allocation_value['new_tipo']
    allocation_value['flag_rateio'] = 0
    allocation_value.drop('new_tipo', axis=1, inplace=True)

    return allocation_value


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
        missing_columns = [col for col in filter_columns if col not in dtfr.columns]

        if missing_columns:
            print(f"""Warning: filter columns missing: {', '.join(missing_columns)}""")
            continue

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

    dtypes = dta.read("fundos_metadata")

    funds = pd.read_excel(f"{xlsx_destination_path}fundos_raw.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(funds, funds)
    funds.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    dtypes = dta.read(f"carteiras_metadata")

    portfolios = pd.read_excel(f"{xlsx_destination_path}carteiras_raw.xlsx",
                               dtype=dtypes)

    equity_stake = compute_equity_stake(portfolios, funds)
    portfolios.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    harmonization_rules = dta.read('harmonization_values_rules')
    harmonize_values(portfolios, harmonization_rules)

    header_daily_values = dta.read('header_daily_values')
    keys_not_allocated = [key for key, value in header_daily_values.items() if value.get('serie', False)]
    types_series = [key for key, value in header_daily_values.items() if value.get('serie', True)]

    proprtnl_allocation = compute_proportional_allocation(portfolios, keys_not_allocated)

    portfolios['flag_rateio'] = portfolios.index.isin(proprtnl_allocation['original_index'].unique()).astype(int)

    portfolios = pd.concat([
        portfolios,
        proprtnl_allocation
    ], ignore_index=True)

    portfolios['valor_calc'] = portfolios['valor_calc'].where(portfolios['flag_rateio'] != 1, 0)

    portfolios['valor_serie'] = portfolios['valor'].where(portfolios['tipo'].isin(types_series), 0)
    portfolios['valor_calc'] = portfolios['valor_calc'].where(~portfolios['tipo'].isin(types_series), 0)

    funds.to_excel(f"{xlsx_destination_path}fundos.xlsx", index=False)
    portfolios.to_excel(f"{xlsx_destination_path}/carteiras.xlsx", index=False)


if __name__ == "__main__":
    main()
