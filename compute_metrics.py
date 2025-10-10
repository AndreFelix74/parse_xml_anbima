#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 15 10:42:26 2025

@author: andrefelix
"""


import pandas as pd


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

    composition = investor[
        (~investor['tipo'].isin(types_to_exclude)) &
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


def compute_equity_stake(investor_holdings, invested):
    """
    Calculate the equity stake of investors based on available quotas and fund values.

    Args:
        investor_holdings (pd.DataFrame): DataFrame containing investor positions,
            with required columns: 'cnpjfundo', 'valor_calc', and 'dtposicao'.
        invested (pd.DataFrame): DataFrame containing fund value data,
            with required columns: 'cnpj', 'valor', 'dtposicao' and a 'tipo' column
            (must be equal to 'quantidade' for inclusion).

    Returns:
        pd.DataFrame: A DataFrame with the calculated 'equity_stake' per investor
            position, indexed by the original investor_holdings index.
    """
    investor_holdings['original_index'] = investor_holdings.index

    columns_invested = ['cnpj', 'valor', 'dtposicao']

    equity_stake = investor_holdings.merge(
        invested[invested['tipo'] == 'patliq'][columns_invested],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner'
    )

    equity_stake.set_index('original_index', inplace=True)

    equity_stake['equity_stake'] = equity_stake['valor_calc'] / equity_stake['valor']

    return equity_stake


def compute(entity, invested, types_series, composition_group_keys):
    """
    Main function for processing fund and portfolio data:
    - Computes equity stake
    """
    investor_holdings_cols = ['cnpjfundo', 'valor_calc', 'dtposicao']

    investor_holdings = entity[entity['cnpjfundo'].notnull()][investor_holdings_cols].copy()

    equity_stake = compute_equity_stake(investor_holdings, invested)
    entity.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']
