#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  7 13:50:35 2025

@author: andrefelix
"""


def _fill_contribution_cols(tree, mask, vl_calc_col='valor_calc_proporcional',
                            rentab_col='rentab_nominal',
                            ativo_col='NEW_NOME_ATIVO_FINAL',
                            tipo_col='NEW_TIPO_FINAL'):
    """
    Assigns contribution-related values to rows in the tree DataFrame
    where the mask condition holds.
    """
    tree.loc[mask, 'contribution_valor_calc'] = tree[vl_calc_col]
    tree.loc[mask, 'contribution_ativo'] = tree[ativo_col]
    tree.loc[mask, 'contribution_composicao'] = (
        tree['contribution_valor_calc']
        / tree['total_invest']
    )
    tree.loc[mask, 'contribution_rentab_nominal'] = tree[rentab_col]
    tree.loc[mask, 'contribution_rentab_ponderada'] = (
        tree[rentab_col]
        * tree['contribution_composicao']
    )
    tree.loc[mask, 'contribution_tipo'] = tree[tipo_col]


def fill_missing_governance_struct(tree_horzt, key_vehicle_governance_struct):
    """
    Fills missing values in the 'KEY_ESTRUTURA_GERENCIAL' column based on whether
    the corresponding 'codcart' value exists in a reference list of valid keys.

    Only rows where 'KEY_ESTRUTURA_GERENCIAL' is null or an empty string are modified.

    Rules:
        - If 'codcart' is in 'key_vehicle_governance_struct', assign 'codcart'
        - Otherwise, assign '#OUTROS'

    Parameters:
        tree_horzt (pd.DataFrame): DataFrame containing the columns:
            - 'KEY_ESTRUTURA_GERENCIAL'
            - 'codcart'

        key_vehicle_governance_struct (Iterable): List or set of valid 'codcart' keys.

    Returns:
        None: Modifies the DataFrame in place.
    """
    missing_struct = (
        tree_horzt['contribution_match'].isna() |
        (tree_horzt['contribution_match'] == '')
    )

    codcart = tree_horzt['codcart'].isin(key_vehicle_governance_struct)

    tree_horzt.loc[missing_struct & codcart, 'contribution_match'] = tree_horzt['codcart']
    tree_horzt.loc[missing_struct & codcart, 'KEY_ESTRUTURA_GERENCIAL'] = tree_horzt['codcart']

    tree_horzt.loc[missing_struct & ~codcart, 'KEY_ESTRUTURA_GERENCIAL'] = '#OUTROS'
    tree_horzt.loc[missing_struct & ~codcart, 'contribution_match'] = '#OUTROS'

    _fill_contribution_cols(tree_horzt, missing_struct)


def assign_estrutura_gerencial_key(tree, key_vehicle_governance_struct, max_deep):
    """
    Assigns governance structure information to the investment tree based on hierarchical levels.

    This function scans all levels of the investment tree, from the deepest level up to the root,
    looking for the first occurrence where:
        - 'cnpjfundo_nivel_{i}' exists in the governance structure list
        - and its corresponding type ('NEW_TIPO_nivel_{i}') is not 'COTAS'

    When such a match is found, the function assigns:
        - 'KEY_ESTRUTURA_GERENCIAL' = 'cnpjfundo_nivel_{i}'

    After all levels are processed, a fallback rule is applied:
        - If 'KEY_ESTRUTURA_GERENCIAL' is still missing
        - And 'cnpjfundo' (level 0) is present and non-empty
        - Then '#OUTROS' is assigned to 'KEY_ESTRUTURA_GERENCIAL'

    Parameters:
        tree (pd.DataFrame): The investment tree in wide format with hierarchical columns.
        key_veiculo_estrutura_gerencial (Iterable): A list or set of fund CNPJs that belong
            to the governance structure.
        max_deep (int): The maximum depth of the investment tree.

    Returns:
        None: The input DataFrame is modified in place.
    """
    group_cols = ['codcart', 'dtposicao', 'cnpb']
    tree['KEY_ESTRUTURA_GERENCIAL'] = None
    tree['contribution_valor_calc'] = 0.0
    tree['contribution_match'] = None

    for i in range(0, max_deep + 1):
        suffix = '' if i == 0 else f'_nivel_{i}'

        cnpj_col = f"cnpjfundo{suffix}"
        vl_calc_col = f"valor_calc{suffix}"
        rentab_col = f"rentab{suffix}"
        ativo_col = f"NEW_NOME_ATIVO{suffix}"
        tipo_col = f"NEW_TIPO{suffix}"

        mask_key_missing = tree['KEY_ESTRUTURA_GERENCIAL'].isna()
        mask_not_marked = tree['contribution_match'].isna()
        mask_in_estrutura = tree[cnpj_col].isin(key_vehicle_governance_struct)
        mask = mask_key_missing & mask_in_estrutura & mask_not_marked

        tree.loc[mask, 'contribution_match'] = tree[cnpj_col]

        first_in_group = ~tree.duplicated(subset=group_cols + [cnpj_col])

        mask &= first_in_group

        tree.loc[mask, 'KEY_ESTRUTURA_GERENCIAL'] = tree[cnpj_col]
        _fill_contribution_cols(tree, mask, vl_calc_col, rentab_col, ativo_col, tipo_col)

    fallback_mask = (
        tree['contribution_match'].isna()
        & tree['cnpjfundo'].notna()
        & (tree['cnpjfundo'] != '')
    )

    tree.loc[fallback_mask, 'contribution_match'] = '#OUTROS'
    tree.loc[fallback_mask, 'KEY_ESTRUTURA_GERENCIAL'] = '#OUTROS'
    _fill_contribution_cols(tree, fallback_mask)


def assign_governance_struct_keys(tree_horzt, governance_struct):
    """
    Assigns governance structure keys to the investment tree based on fund identifiers.

    This function enriches the investment tree with governance structure information by:
        - Scanning all hierarchical levels to identify the first occurrence of a fund
          (cnpjfundo_nivel_{i}) present in the governance structure list and assigning it
          to 'KEY_ESTRUTURA_GERENCIAL'.
        - Filling in any remaining missing values in 'KEY_ESTRUTURA_GERENCIAL' based on
          whether the 'codcart' field exists in the governance structure list.
          If not, '#OUTROS' is assigned as a fallback.

    Parameters:
        tree_horzt (pd.DataFrame): The horizontal investment tree containing fund relationships
            and hierarchical depth.
        governance_struct (pd.DataFrame): A DataFrame containing governance structure
            definitions, including the column 'KEY_VEICULO'.

    Returns:
        None: The function modifies the input DataFrame in place.
    """
    key_vehicle_governance_struct = governance_struct['KEY_VEICULO'].dropna().unique()

    max_deep = tree_horzt['nivel'].max()

    assign_estrutura_gerencial_key(tree_horzt, key_vehicle_governance_struct, max_deep)

    fill_missing_governance_struct(tree_horzt, key_vehicle_governance_struct)
