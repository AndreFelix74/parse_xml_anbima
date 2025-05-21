#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 14 16:55:27 2025

@author: andrefelix
"""


import os
import networkx as nx
import pandas as pd
import util as utl
import data_access as dta


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
    current.loc[mask, 'classeoperacao'] = current.loc[mask, f"classeoperacao_nivel_{deep+1}"]
    current.loc[mask, 'dtvencimento'] = current.loc[mask, f"dtvencimento_nivel_{deep+1}"]
    current.loc[mask, 'dtvencativo'] = current.loc[mask, f"dtvencativo_nivel_{deep+1}"]
    current.loc[mask, 'compromisso_dtretorno'] = current.loc[mask, f"compromisso_dtretorno_nivel_{deep+1}"]


def validate_fund_graph_is_acyclic(funds):
    """
    Validates that the fund-to-fund relationships form a Directed Acyclic Graph (DAG).
    Raises an exception if any cycles are found in the investment structure.

    Args:
        funds (pd.DataFrame): DataFrame containing at least 'cnpj' (invested fund) and
                              'cnpjfundo' (investor fund) columns.

    Raises:
        ValueError: If a cycle is detected in the graph of fund relationships.
    """
    edges = (
        funds[['cnpjfundo', 'cnpj']]
        .dropna()
        .drop_duplicates()
        .values
        .tolist()
    )
    graph = nx.DiGraph()
    graph.add_edges_from(edges)

    try:
        nx.algorithms.dag.topological_sort(graph)
    except nx.NetworkXUnfeasible:
        cycle = nx.find_cycle(graph, orientation='original')
        raise ValueError(f"Cycle detected in fund relationships: {cycle}")


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
    if 'cnpjfundo' in portfolios.columns:
        portfolios.rename(columns={'cnpjfundo': f"cnpjfundo_nivel_{deep}"}, inplace=True)

    if portfolios[f"cnpjfundo_nivel_{deep}"].notna().sum() == 0:
        return portfolios

    current = portfolios.merge(
        funds,
        left_on=[f"cnpjfundo_nivel_{deep}", 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='left',
        suffixes=('', f"_nivel_{deep+1}"),
        indicator=True
    )

    mask = current['_merge'] == 'both'

    _apply_calculations_to_new_rows(current, mask, deep)

    current.drop(columns=['_merge'], inplace=True)

    current.rename(columns={'cnpjfundo': f"cnpjfundo_nivel_{deep+1}"}, inplace=True)

    return build_tree_horizontal(current, funds, deep + 1)


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

    current['deep'] += 1
    current[f"equity_stake"] *= current['equity_stake_portfolio']

    return [current[portfolios.columns]] + build_tree_branchs(current[portfolios.columns], funds)


def build_tree_leaves(tree, funds):
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
    leaves = tree.merge(
        funds[funds['tipo'] != 'cotas'],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner',
        suffixes=('_tree', '')
    )

    leaves['deep'] = leaves['deep'] + 1
    leaves['valor_calc'] *= leaves['equity_stake']

    return leaves[tree.columns]


def main():
    """
    Main execution function for loading portfolio and fund data, constructing
    the horizontal investment tree, and exporting the final tree structure to
    an Excel file.
    """
    config = utl.load_config('config.ini')

    xlsx_aux_path = config['Paths']['data_aux_path']
    xlsx_aux_path = f"{os.path.dirname(utl.format_path(xlsx_aux_path))}/"

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    cols_funds = ['cnpj', 'dtposicao', 'tipo', 'cnpjfundo', 'equity_stake',
                  'valor_calc', 'composicao', 'isin', 'classeoperacao',
                  'dtvencimento', 'dtvencativo', 'compromisso_dtretorno',
                  'NEW_TIPO', 'coupom', 'qtd', 'quantidade', 'fNUMERACA.DESCRICAO',
                  'fNUMERACA.TIPO_ATIVO', 'fEMISSOR.NOME_EMISSOR', 'NEW_TIPO',
                  'DATA_VENC_TPF', 'ANO_VENC_TPF', 'dCadFI_CVM.TP_FUNDO',
                  'dCadFI_CVM.RENTAB_FUNDO', 'dCadFI_CVM.CLASSE_ANBIMA', 'NEW_NOME_ATIVO',
                  'NEW_GESTOR']
 
    dtypes = dta.read("fundos_metadata")
    funds = pd.read_excel(f"{xlsx_destination_path}fundos.xlsx",
                          dtype=dtypes)

    validate_fund_graph_is_acyclic(funds)

    funds = funds[funds['valor_serie'] == 0][cols_funds].copy()

    cols_port = ['cnpjcpf', 'codcart', 'cnpb', 'dtposicao', 'nome', 'tipo',
                 'cnpjfundo', 'equity_stake', 'valor_calc', 'composicao', 'isin',
                 'classeoperacao', 'dtvencimento']

    dtypes = dta.read(f"carteiras_metadata")
    portfolios = pd.read_excel(f"{xlsx_destination_path}carteiras.xlsx",
                               dtype=dtypes)

    portfolios = portfolios[(portfolios['flag_rateio'] == 0) &
                            (portfolios['valor_serie'] == 0)][cols_port].copy()

    portfolios['dtvencativo'] = ''
    portfolios['compromisso_dtretorno'] = ''

    tree = build_tree_horizontal(portfolios, funds)

    tree.to_excel(f"{xlsx_destination_path}/arvore_carteiras.xlsx", index=False)


if __name__ == "__main__":
    main()
