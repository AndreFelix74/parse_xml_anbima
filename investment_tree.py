#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 14 16:55:27 2025

@author: andrefelix
"""


def _apply_calculations_to_new_rows(current, mask, deep):
    """
    Applies in-place calculations to rows resulting from a successful merge
    ('both') at the current recursion depth.

    Args:
        current (pd.DataFrame): DataFrame containing the merged investment tree
        structure.
        mask (pd.Series): Boolean Series identifying the rows that originated
        from both sides of the merge.
        deep (int): Current recursion depth, used to access level-specific columns.
    """
    current.loc[mask, 'nivel'] = deep + 1
    current.loc[mask, 'equity_stake'] *= current.loc[mask, f"equity_stake_nivel_{deep+1}"].fillna(1)
    current.loc[mask, 'valor_calc'] = (
        current.loc[mask, f"valor_calc_nivel_{deep+1}"]
        * current.loc[mask, 'equity_stake'].fillna(1)
    )

    current.loc[mask, 'composicao'] *= current.loc[mask, f"composicao_nivel_{deep+1}"].fillna(1)
    current.loc[mask, 'isin'] = current.loc[mask, f"isin_nivel_{deep+1}"]

    mask_estrutura = mask & current[f"IS_CNPJFUNDO_ESTRUTURA_GERENCIAL_nivel_{deep+1}"]

    current.loc[mask_estrutura, 'KEY_ESTRUTURA_GERENCIAL'] = current.loc[
        mask_estrutura,
        f"cnpj_nivel_{deep+1}"
    ]
    current.loc[mask_estrutura, 'NEW_TIPO_ESTRUTURA_GERENCIAL'] = current.loc[
        mask_estrutura,
        f"NEW_TIPO_nivel_{deep+1}"
    ]

    sufix = '' if deep == 0 else f"_nivel_{deep}"
    current.loc[mask, 'PARENT_FUNDO'] = current.loc[mask, f"NEW_NOME_ATIVO{sufix}"]
    current.loc[mask, 'PARENT_FUNDO_GESTOR'] = current.loc[mask, f"NEW_GESTOR{sufix}"]


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

    _apply_calculations_to_new_rows(current, mask, deep)

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


def create_column_based_on_levels(tree_hrzt, new_col, base_col, deep):
    """
    Preenche uma nova coluna com valores prioritários entre colunas base e níveis sucessivos.

    Args:
        tree_hrzt (pd.DataFrame): DataFrame de entrada.
        new_col (str): Nome da nova coluna a ser criada.
        base_col (str): Nome da coluna base.
        deep (int): Número de níveis (sufixos _nivel_1 a _nivel_{deep}).

    Returns:
        pd.DataFrame: O DataFrame com a nova coluna preenchida.
    """
    priority_cols = [f"{base_col}_nivel_{i}" for i in range(deep, 0, -1)]
    priority_cols.append(base_col)

    tree_hrzt[new_col] = tree_hrzt[priority_cols].bfill(axis=1).iloc[:, 0]


def fill_level_columns_forward(tree_hrzt, base_col, deep):
    """
    Forward-fills missing values in level columns by cascading values from higher to lower levels.

    Args:
        tree_hrzt (pd.DataFrame): The DataFrame with hierarchical level columns.
        base_col (str): Base name of the columns (e.g., 'estrutura').
        deep (int): Number of levels (e.g., 4 means columns from _nivel_1 to _nivel_4).

    Returns:
        pd.DataFrame: The modified DataFrame with levels filled hierarchically.
    """
    for i in range(0, deep):
        curr_level_suffix = f"_nivel_{i}" if i > 0 else ''
        current_col = f"{base_col}{curr_level_suffix}"
        next_col = f"{base_col}_nivel_{i+1}"

        mask = tree_hrzt[next_col].isna() | (tree_hrzt[next_col] == '')
        tree_hrzt.loc[mask, next_col] = tree_hrzt.loc[mask, current_col]


def generate_final_columns(tree_horzt):
    """
    Generate final columns based on hierarchical levels in the 'tree_horzt' DataFrame.

    This function calculates the maximum depth of the hierarchy using the 'nivel' column
    and uses it to generate final versions of multiple columns by aggregating or selecting
    values based on that depth.

    Parameters:
    ----------
    tree_horzt : pandas.DataFrame
        The hierarchical tree DataFrame containing the 'nivel' column and intermediate
        columns used to compute final columns.

    Returns:
    -------
    None
        The function modifies the input DataFrame in place, adding new final columns.
    """
    max_deep = tree_horzt['nivel'].max()

    columns_to_generate = [
        'NEW_TIPO', 'NEW_NOME_ATIVO', 'NEW_GESTOR_WORD_CLOUD', 'fEMISSOR.NOME_EMISSOR'
    ]

    for base_col in columns_to_generate:
        create_column_based_on_levels(tree_horzt, f"{base_col}_FINAL", base_col, max_deep)


def fill_missing_estrutura_gerencial(tree_horzt, key_veiculo_estrutura_gerencial):
    """
    Fills missing values in the 'KEY_ESTRUTURA_GERENCIAL' column based on whether
    the corresponding 'codcart' value exists in a reference list of valid keys.

    Only rows where 'KEY_ESTRUTURA_GERENCIAL' is null or an empty string are modified.

    Rules:
        - If 'codcart' is in 'key_veiculo_estrutura_gerencial', assign 'codcart'
        - Otherwise, assign '#OUTROS'

    Parameters:
        tree_horzt (pd.DataFrame): DataFrame containing the columns:
            - 'KEY_ESTRUTURA_GERENCIAL'
            - 'codcart'

        key_veiculo_estrutura_gerencial (Iterable): List or set of valid 'codcart' keys.

    Returns:
        None: Modifies the DataFrame in place.
    """
    sem_estrutura = (
        tree_horzt['KEY_ESTRUTURA_GERENCIAL'].isna() |
        (tree_horzt['KEY_ESTRUTURA_GERENCIAL'] == '')
    )

    codcart = tree_horzt['codcart'].isin(key_veiculo_estrutura_gerencial)

    tree_horzt.loc[sem_estrutura & codcart, 'KEY_ESTRUTURA_GERENCIAL'] = \
        tree_horzt.loc[sem_estrutura & codcart, 'codcart']

    tree_horzt.loc[sem_estrutura & ~codcart, 'KEY_ESTRUTURA_GERENCIAL'] = '#OUTROS'


def enrich_tree(tree_horzt, governance_struct):
    """
    Enriches a tree structure with derived textual fields and governance structure mappings.

    This function:
        - Generates final label columns.
        - Propagates level-based data forward.
        - Creates a combined search string from relevant textual fields.
        - Attempts to fill in missing governance structure information.

    Args:
        tree_horzt (pd.DataFrame): The horizontal tree structure containing fund
        relationships.
        governance_struct (pd.DataFrame): Governance structure dataset with
        'KEY_VEICULO' identifiers.

    Returns:
        None: The input DataFrame is modified in-place.
    """
    key_vehicle_governance_struct = governance_struct['KEY_VEICULO'].dropna().unique()

    generate_final_columns(tree_horzt)
    max_deep = tree_horzt['nivel'].max()
    fill_level_columns_forward(tree_horzt, 'NEW_NOME_ATIVO', max_deep)

    tree_horzt['SEARCH'] = (
        tree_horzt['NEW_NOME_ATIVO_FINAL'].fillna('')
        + ' ' + tree_horzt['NEW_GESTOR_WORD_CLOUD_FINAL'].fillna('')
        + ' ' + tree_horzt['fEMISSOR.NOME_EMISSOR_FINAL'].fillna('')
        + ' ' + tree_horzt['PARENT_FUNDO'].fillna('')
    )

    fill_missing_estrutura_gerencial(tree_horzt, key_vehicle_governance_struct)


def build_tree(funds, portfolios, governance_struct):
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
        governance_struct (pd.DataFrame): Governance structure dataset with
        'KEY_VEICULO' identifiers.

    Returns:
        pd.DataFrame: A combined DataFrame representing the horizontal fund tree.
    """
    key_vehicle_governance_struct = governance_struct['KEY_VEICULO'].dropna().unique()

    cols_funds = ['cnpj', 'dtposicao', 'cnpjfundo', 'equity_stake', 'composicao',
                  'valor_calc', 'isin', 'NEW_TIPO', 'fNUMERACA.DESCRICAO',
                  'fEMISSOR.NOME_EMISSOR', 'NEW_NOME_ATIVO', 'NEW_GESTOR',
                  'NEW_GESTOR_WORD_CLOUD', 'IS_CNPJFUNDO_ESTRUTURA_GERENCIAL']

    funds['IS_CNPJFUNDO_ESTRUTURA_GERENCIAL'] = funds['cnpj'].isin(key_vehicle_governance_struct)
    funds['NEW_TIPO_ESTRUTURA_GERENCIAL'] = funds['NEW_TIPO']

    funds = funds[funds['valor_serie'] == 0][cols_funds].copy()

    cols_port = ['cnpjcpf', 'codcart', 'cnpb', 'dtposicao', 'nome', 'cnpjfundo',
                 'equity_stake', 'composicao', 'valor_calc', 'isin',
                 'NEW_TIPO', 'NEW_NOME_ATIVO', 'fEMISSOR.NOME_EMISSOR', 'NEW_GESTOR',
                 'NEW_GESTOR_WORD_CLOUD']

    portfolios = portfolios[(portfolios['flag_rateio'] == 0) &
                            (portfolios['valor_serie'] == 0)][cols_port].copy()

    portfolios['nivel'] = 0
    portfolios['fNUMERACA.DESCRICAO'] = ''
    portfolios['cnpj'] = ''
    portfolios['NEW_TIPO_ESTRUTURA_GERENCIAL'] = portfolios['NEW_TIPO']

    mask_estrutura = portfolios['cnpjfundo'].isin(key_vehicle_governance_struct)
    portfolios['IS_CNPJFUNDO_ESTRUTURA_GERENCIAL'] = portfolios['cnpjfundo'].isin(
        key_vehicle_governance_struct
    )
    portfolios.loc[mask_estrutura, 'KEY_ESTRUTURA_GERENCIAL'] = (
        portfolios.loc[mask_estrutura, 'cnpjfundo']
    )

    return build_tree_horizontal(portfolios.copy(), funds)
