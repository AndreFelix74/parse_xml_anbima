#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  7 13:50:35 2025

@author: andrefelix
"""


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
        tree_horzt['KEY_ESTRUTURA_GERENCIAL'].isna() |
        (tree_horzt['KEY_ESTRUTURA_GERENCIAL'] == '')
    )

    codcart = tree_horzt['codcart'].isin(key_vehicle_governance_struct)

    tree_horzt.loc[missing_struct & codcart, 'KEY_ESTRUTURA_GERENCIAL'] = \
        tree_horzt.loc[missing_struct & codcart, 'codcart']

    tree_horzt.loc[missing_struct & ~codcart, 'KEY_ESTRUTURA_GERENCIAL'] = '#OUTROS'


def assign_estrutura_gerencial_key(tree, key_vehicle_governance_struct, max_deep):
    """
    Atribui as colunas 'KEY_ESTRUTURA_GERENCIAL' e 'NEW_TIPO_ESTRUTURA_GERENCIAL' com base
    no primeiro nível (do mais profundo ao mais superficial) em que o CNPJ do fundo investidor
    ('cnpjfundo_nivel_{i}') pertence à estrutura gerencial.

    Args:
        tree (pd.DataFrame): DataFrame da árvore horizontal.
        key_vehicle_governance_struct (Iterable): Conjunto de CNPJs da estrutura gerencial.
        max_deep (int): Profundidade máxima da árvore.

    Returns:
        None: Modifica o DataFrame in-place.
    """
    tree['KEY_ESTRUTURA_GERENCIAL'] = None
    tree['NEW_TIPO_ESTRUTURA_GERENCIAL'] = None

    for i in range(max_deep, -1, -1):
        cnpj_col = 'cnpjfundo' if i == 0 else f'cnpjfundo_nivel_{i}'
        tipo_col = 'NEW_TIPO' if i == 0 else f'NEW_TIPO_nivel_{i}'

        mask_key_missing = tree['KEY_ESTRUTURA_GERENCIAL'].isna()
        mask_in_estrutura = tree[cnpj_col].isin(key_vehicle_governance_struct)
        mask = mask_key_missing & mask_in_estrutura

        tree.loc[mask, 'KEY_ESTRUTURA_GERENCIAL'] = tree.loc[mask, cnpj_col]
        tree.loc[mask, 'NEW_TIPO_ESTRUTURA_GERENCIAL'] = tree.loc[mask, tipo_col]


def assign_governance_struct_keys(tree_horzt, governance_struct):
    """
    Assigns governance structure keys to the investment tree based on fund identifiers.

    This function enriches the investment tree with governance structure information by:
        - Scanning all hierarchical levels to identify the first occurrence of a fund
          (cnpjfundo_nivel_{i}) present in the governance structure list and assigning it
          to 'KEY_ESTRUTURA_GERENCIAL' and 'NEW_TIPO_ESTRUTURA_GERENCIAL'.
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
