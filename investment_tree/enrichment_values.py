#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  7 12:58:01 2025

@author: andrefelix
"""


def accumulate_columns_by_level(tree_hrzt, result_col, base_col, deep):
    """
    Accumulates numerical values from level-specific columns by multiplying them,
    producing a final consolidated value.

    Each level column is expected to follow the pattern '{base_col}_nivel_{i}' 
    for i in range(1, deep + 1). Missing values are treated as 1.0 (neutral element).

    Args:
        df (pd.DataFrame): DataFrame containing level-specific columns.
        base_col (str): Name of the base column (e.g., 'equity_stake').
        deep (int): Maximum depth to consider for level-specific columns.
        result_col (str): Name of the resulting column to store the accumulated value.

    Returns:
        None: Modifies the DataFrame in place by adding the result_col.
    """
    accumulate_cols = [f"{base_col}_nivel_{i}" for i in range(1, deep + 1)]
    accumulate_cols.append(base_col)
    tree_hrzt[result_col] = tree_hrzt[accumulate_cols].fillna(1).prod(axis=1)


def compute_proportional_value(tree_horzt, max_depth):
    """
    Compute proportional investment values for each hierarchy level.

    For every level from 0 to max_depth:
        - Select rows matching the level.
        - Retrieve the corresponding `value_calc[_level_i]` column.
        - Multiply it by the pre-computed accumulated equity stake.
        - Store the result in `proportional_value`.

    Args:
        tree_horzt (pd.DataFrame): Input DataFrame containing hierarchical
            relationships, `level`, `value_calc[_level_i]`, and `equity_stake`.
        max_depth (int): Maximum depth of levels.

    Returns:
        None: Modifies the input DataFrame in place by adding/updating
        `proportional_value`.
    """
    for i in range(0, max_depth + 1):
        suffix = '' if i == 0 else f"_nivel_{i}"
        mask_level = tree_horzt['nivel'] == i
        col_name = f"valor_calc{suffix}"
        tree_horzt.loc[mask_level, 'valor_calc_proporcional'] = (
            tree_horzt.loc[mask_level, col_name]
            * tree_horzt.loc[mask_level, 'equity_stake_leaf']
            * tree_horzt.loc[mask_level, 'pct_submassa_isin_cnpb'].fillna(1.0)
        )


def compute_weighted_returns(tree_horzt, max_depth):
    """
    Compute weighted and nominal returns for each hierarchy level.

    For every level from max_depth down to 0:
        - Select rows matching the level.
        - Retrieve the corresponding `return[_level_i]` column.
        - Calculate weighted return (`weighted_return`) as composition × return.
        - Assign nominal return (`nominal_return`) as the raw level return.

    Args:
        tree_horzt (pd.DataFrame): Input DataFrame containing hierarchical
            relationships, `level`, `composition`, and `return[_level_i]`.
        max_depth (int): Maximum depth of levels.

    Returns:
        None: Modifies the input DataFrame in place by adding/updating
        `weighted_return` and `nominal_return`.
    """
    for i in range(max_depth, -1, -1):
        mask_level = tree_horzt['nivel'] == i
        suffix = '' if i == 0 else f"_nivel_{i}"
        col_returns = f"rentab{suffix}"
        tree_horzt.loc[mask_level, 'rentab_ponderada'] = (
            tree_horzt.loc[mask_level, 'composicao']
            * tree_horzt.loc[mask_level, col_returns].fillna(0.0)
            * tree_horzt.loc[mask_level, 'pct_submassa_isin_cnpb'].fillna(1.0)
        )
        tree_horzt.loc[mask_level, 'rentab_nominal'] = tree_horzt.loc[mask_level, col_returns]


def enrich_values(tree_horzt):
    """
    Enriches the tree DataFrame with derived numerical fields.

    This function:
        - Accumulates equity stakes across hierarchical levels by multiplying
          level-specific participation columns.
        - Computes proportional investment values (`valor_calc_proporcional`)
          per entity and level.
        - Aggregates total invested value per fund/date (`total_invest`) and
          calculates each entity's composition share (`composicao`).
        - Derives weighted and nominal returns (`rentab_ponderada`,
          `rentab_nominal`) based on level-specific return columns.

    Args:
        tree_horzt (pd.DataFrame): Input DataFrame containing hierarchical
            fund/asset relationships, level-based value columns, and return data.

    Returns:
        None: Modifies the input DataFrame in place by adding derived numerical
        columns (`equity_stake`, `valor_calc_proporcional`, `total_invest`,
        `composicao`, `rentab_ponderada`, `rentab_nominal`).
    """
    max_depth = tree_horzt['nivel'].max()

    accumulate_columns_by_level(tree_horzt, 'equity_stake_leaf', 'equity_stake', max_depth)

    compute_proportional_value(tree_horzt, max_depth)

    tree_horzt['total_invest'] = (
        tree_horzt.groupby(['cnpb', 'CLCLI_CD', 'dtposicao'])['valor_calc_proporcional'].transform('sum')
    )

    tree_horzt['composicao'] = (
        tree_horzt['valor_calc_proporcional']
        / tree_horzt['total_invest']
    )

    compute_weighted_returns(tree_horzt, max_depth)
foo
