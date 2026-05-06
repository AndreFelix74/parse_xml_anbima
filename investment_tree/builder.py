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
    current['valor_calc_propocional'] = current['valor_calc'] * current['equity_stake_portfolio']

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
    leaves['valor_calc_proporcional'] *= leaves['equity_stake_tree']

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
                  'isin', 'NEW_TIPO', 'fNUMERACA.DESCRICAO', 'fNUMERACA.TIPO_ATIVO',
                  'fEMISSOR.NOME_EMISSOR', 'NEW_NOME_ATIVO', 'NEW_GESTOR',
                  'NEW_GESTOR_WORD_CLOUD', 'rentab', 'caracteristica']

    funds = funds[funds['valor_serie'] == 0][['cnpj'] + cols_common].copy()

    cols_port = ['cnpjcpf', 'codcart', 'cnpb']

    portfolios = portfolios[(portfolios['flag_rateio'] == 0) &
                            (portfolios['valor_serie'] == 0)][cols_port + cols_common].copy()

    portfolios['nivel'] = 0
    portfolios['cnpj'] = ''

    return build_tree_horizontal(portfolios.copy(), funds)


def explode_horizontal_tree_submassa(tree_horzt_sub, port_submassa):
    """
    Propagates submassa attribution down the horizontal investment tree by
    matching each level's ``isin`` (``isin``, ``isin_nivel_1``, ...) against
    ``port_submassa`` and copying the submassa columns
    (``COD_SUBMASSA``, ``SUBMASSA``, ``pct_submassa_isin_cnpb``, ``CODCART``)
    onto the matched rows.

    Rows whose submassa cannot be resolved at any level fall back to
    ``COD_SUBMASSA = '1'`` / ``SUBMASSA = 'BSPS'`` and a participation of
    ``1.0``.

    Parameters
    ----------
    tree_horzt_sub : pandas.DataFrame
        Horizontal tree restricted to (cnpb, dtposicao) pairs covered by
        ``port_submassa``.
    port_submassa : pandas.DataFrame
        Submassa-tagged portfolios DataFrame with composition columns already
        computed (see ``compute_composition_portfolio_submassa``).

    Returns
    -------
    pandas.DataFrame
        ``tree_horzt_sub`` enriched with the submassa columns above.
    """
    cols_port_submassa = ['dtposicao', 'CNPB', 'isin', 'CODCART',
                          'COD_SUBMASSA', 'SUBMASSA', 'pct_submassa_isin_cnpb']
    mask_port = (~port_submassa['isin'].isna())

    tree_horzt_sub['COD_SUBMASSA'] = None
    tree_horzt_sub['SUBMASSA'] = None
    tree_horzt_sub['pct_submassa_isin_cnpb'] = 1.0
    tree_horzt_sub['CODCART'] = None

    max_depth = tree_horzt_sub['nivel'].max()

    #max_depth != max_depth pega o caso float('nan'), 
    # pois que NaN eh o único valor que nao eh igual a si mesmo.
    if max_depth is None or max_depth != max_depth:
        return tree_horzt_sub

    for i in range(0, max_depth + 1):
        suffix = '' if i == 0 else f'_nivel_{i}'
        isin_col = f"isin{suffix}"

        tree_horzt_sub = tree_horzt_sub.merge(
            port_submassa[mask_port][cols_port_submassa],
            left_on=['dtposicao', 'cnpb', isin_col],
            right_on=['dtposicao', 'CNPB', 'isin'],
            how='left',
            suffixes=('', f"_{suffix}"),
            indicator=True,
        )

        mask_merge = (tree_horzt_sub['_merge'] == 'both')

        tree_horzt_sub.loc[mask_merge, 'COD_SUBMASSA'] = tree_horzt_sub[f"COD_SUBMASSA_{suffix}"]
        tree_horzt_sub.loc[mask_merge, 'SUBMASSA'] = tree_horzt_sub[f"SUBMASSA_{suffix}"]
        tree_horzt_sub.loc[mask_merge, 'pct_submassa_isin_cnpb'] = tree_horzt_sub[f"pct_submassa_isin_cnpb_{suffix}"]
        tree_horzt_sub.loc[mask_merge, 'CODCART'] = tree_horzt_sub[f"CODCART_{suffix}"]

        tree_horzt_sub.drop(columns=['_merge'], inplace=True)

    tree_horzt_sub['pct_submassa_isin_cnpb'] = tree_horzt_sub['pct_submassa_isin_cnpb'].astype(float).fillna(1.0)
    mask_bsps = (tree_horzt_sub['SUBMASSA'].isna())
    tree_horzt_sub.loc[mask_bsps, 'COD_SUBMASSA'] = '1'
    tree_horzt_sub.loc[mask_bsps, 'SUBMASSA'] = 'BSPS'

    return tree_horzt_sub
