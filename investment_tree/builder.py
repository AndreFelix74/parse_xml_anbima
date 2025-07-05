#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 14 16:55:27 2025

@author: andrefelix
"""


def build_tree_horizontal(portfolios, funds, deep=0):
    """
    Recursively builds a horizontal investment tree by merging portfolio and
    fund data across nested levels.

    Args:
        portfolios (pd.DataFrame): DataFrame containing portfolio information.
        funds (pd.DataFrame): DataFrame containing fund composition data.
        deep (int): Current level of recursion (depth in the investment chain).

    Returns:
        pd.DataFrame: A single wide-format DataFrame with expanded investment
        layers and calculated stakes.
    """
    left_col = 'cnpjfundo' if deep == 0 else f"cnpjfundo_nivel_{deep}"

    if portfolios[left_col].notna().sum() == 0:
        return portfolios

    current = portfolios.merge(
        funds,
        left_on=[left_col, 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='left',
        suffixes=('', f"_nivel_{deep+1}"),
        indicator=True
    )

    mask = current['_merge'] == 'both'

    current.loc[mask, 'nivel'] = deep + 1

    #as tres linhas abaixo devem ser movidas para o enriquecimento.
    sufix = '' if deep == 0 else f"_nivel_{deep}"
    current.loc[mask, 'PARENT_FUNDO'] = current.loc[mask, f"NEW_NOME_ATIVO{sufix}"]
    current.loc[mask, 'PARENT_FUNDO_GESTOR'] = current.loc[mask, f"NEW_GESTOR{sufix}"]

    current.drop(columns=['_merge'], inplace=True)

    return build_tree_horizontal(current, funds, deep + 1)


def build_assets_tree_horizontal(total_assets, tree_vertical, deep=0):
    """
    Recursively builds a horizontal investment tree by merging portfolio and
    fund data across nested levels.

    Args:
        portfolios (pd.DataFrame): DataFrame containing portfolio information.
        funds (pd.DataFrame): DataFrame containing fund composition data.
        deep (int): Current level of recursion (depth in the investment chain).

    Returns:
        pd.DataFrame: A single wide-format DataFrame with expanded investment
        layers and calculated stakes.
    """
    if deep == 0:
        return total_assets

    current = total_assets.merge(
        tree_vertical[tree_vertical['nivel'] == deep],
        on=['NEW_TIPO', 'NEW_NOME_ATIVO', 'dtposicao', 'dtposicao', 'codcart', 'nome', 'cnpb'],
        how='left',
        suffixes=('', f"_nivel_{deep}")
    )

    return build_assets_tree_horizontal(current, tree_vertical, deep - 1)


def build_tree_branchs(portfolios, funds):
    """
    Recursively builds a list of vertically-stacked DataFrames representing
    each level of fund-of-funds investment.

    Args:
        portfolios (pd.DataFrame): DataFrame containing portfolio data at the
        current level.
        funds (pd.DataFrame): DataFrame with fund composition data, filtered to
        include only fund investments ("cotas").

    Returns:
        list[pd.DataFrame]: List of DataFrames representing each level in the
        recursive investment chain.
    """
    current = portfolios.merge(
        funds[funds['tipo'] == 'cotas'],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner',
        suffixes=('_portfolio', '')
    )

    if current.empty:
        return []

    current['nivel'] += 1
    current['equity_stake'] *= current['equity_stake_portfolio']
    current['valor_calc'] = current['valor_calc'] * current['equity_stake_portfolio']

    return [current[portfolios.columns]] + build_tree_branchs(current[portfolios.columns], funds)


def build_tree_leaves(tree_branchs, funds):
    """
    Extracts and processes leaf-level assets (non-fund investments) from the
    recursive investment tree.

    Args:
        tree (pd.DataFrame): Investment tree DataFrame containing nested
        fund-of-funds structure.
        funds (pd.DataFrame): DataFrame containing all funds, including both
        fund and non-fund assets.

    Returns:
        pd.DataFrame: A DataFrame of leaf nodes with updated calculated values,
        saved to 'leaves.xlsx'.
    """
    leaves = tree_branchs.merge(
        funds[funds['tipo'] != 'cotas'],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner',
        suffixes=('_tree', '')
    )

    leaves['nivel'] = leaves['nivel'] + 1
    leaves['valor_calc'] *= leaves['equity_stake_tree']

    return leaves[tree_branchs.columns]


def build_tree(funds, portfolios):
    """
    Builds a horizontal tree of fund relationships by combining fund and portfolio data.

    This function:
        - Filters and prepares the funds and portfolios datasets.
        - Identifies which entities are part of a governance structure.
        - Adds relevant columns and flags for hierarchy construction.
        - Combines both datasets and returns the horizontal tree.

    Args:
        funds (pd.DataFrame): DataFrame containing fund-to-fund relationships.
        portfolios (pd.DataFrame): DataFrame containing investor portfolios and
        allocations.

    Returns:
        pd.DataFrame: A combined DataFrame representing the horizontal fund tree.
    """
    cols_common = ['dtposicao', 'cnpjfundo', 'nome', 'equity_stake', 'valor_calc',
                  'isin', 'NEW_TIPO', 'fNUMERACA.DESCRICAO', 'fEMISSOR.NOME_EMISSOR',
                  'NEW_NOME_ATIVO', 'NEW_GESTOR', 'NEW_GESTOR_WORD_CLOUD', 'rentab']

    funds = funds[funds['valor_serie'] == 0][['cnpj'] + cols_common].copy()

    cols_port = ['cnpjcpf', 'codcart', 'cnpb']

    portfolios = portfolios[(portfolios['flag_rateio'] == 0) &
                            (portfolios['valor_serie'] == 0)][cols_port + cols_common].copy()

    portfolios['nivel'] = 0
    portfolios['cnpj'] = ''

    return build_tree_horizontal(portfolios.copy(), funds)
