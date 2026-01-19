#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 31 10:37:20 2025

@author: andrefelix
"""


def check_puposicao_vs_valorcota(investor_holdings, invested):
    """
    Compares the 'puposicao' field in investor holdings with the 'valor' field 
    from invested data where 'tipo' is 'valorcota', for matching fund CNPJs and dates.

    Parameters:
    ----------
    investor_holdings : pandas.DataFrame
        DataFrame containing investor fund holdings with at least the columns:
        'cnpjfundo', 'dtposicao', and 'puposicao'.

    invested : pandas.DataFrame
        DataFrame containing invested values with at least the columns:
        'cnpj', 'valor', 'dtposicao', and 'tipo'. Only rows where 'tipo' == 'valorcota' 
        are used for comparison.

    Returns:
    -------
    pandas.DataFrame
        A merged DataFrame including a boolean column 'puposicao_igual_valor' that 
        indicates whether 'puposicao' and 'valor' are equal for each matched row.
    """
    investor_holdings['original_index'] = investor_holdings.index

    cols_invested = ['cnpj', 'valor', 'dtposicao']

    compare_puposicao = investor_holdings.merge(
        invested[invested['tipo'] == 'valorcota'][cols_invested],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner'
    )

    compare_puposicao.set_index('original_index', inplace=True)

    decimal_places = 8

    mask_diff = (
        round(compare_puposicao['puposicao'], decimal_places)
        != round(compare_puposicao['valor'], decimal_places)
    )

    return compare_puposicao.loc[mask_diff]


def check_composition_consistency(entity, group_keys, min_pct_diff=0.0):
    """
    Checks consistency between the computed total value of investments and 
    the declared net asset value ('patrimônio líquido') for each group.

    For each group defined by `group_keys` (plus 'dtposicao'), this function:
    - Computes the total investment value (`valor_calc`).
    - Extracts the declared net asset value (`valor_serie`) for rows where `tipo == 'patliq'`.
    - Compares both values and returns only the rows where the absolute percentage difference exceeds `min_pct_diff`.

    Parameters
    ----------
    entity : pandas.DataFrame
        DataFrame containing portfolio data with at least the columns:
        'tipo', 'valor_calc', 'valor_serie', and the grouping keys.
    
    group_keys : list of str
        List of columns to group by. 'dtposicao' will be added automatically if not present.
    
    min_pct_diff : float, optional
        Minimum absolute percentage difference (e.g., 0.01 for 1%) to include a row in the result.
        Default is 0.0 (returns all non-zero percentage differences).

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing group keys, 'valor_serie', 'total_invest', 'diff',
        and 'pct_diff', only for the groups where abs(pct_diff) > min_pct_diff.
    """
    if 'dtposicao' not in group_keys:
        group_keys = group_keys + ['dtposicao']

    total_assets = (
        entity[group_keys + ['valor_calc']]
        .groupby(group_keys, as_index=False)
        .sum()
        .rename(columns={'valor_calc': 'total_invest'})
    )

    patliq = entity[entity['tipo'] == 'patliq'][group_keys + ['valor_serie']].copy()

    check = total_assets.merge(patliq, on=group_keys, how='left')

    check['diff'] = check['total_invest'] - check['valor_serie']
    check['pct_diff'] = check['diff'] / check['valor_serie']

    return check[check['pct_diff'].abs() > min_pct_diff]
