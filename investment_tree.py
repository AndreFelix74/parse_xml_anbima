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
import file_handler as fhdl


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
    file_ext = 'xlsx' #config['Paths'].get('destination_file_extension', 'xlsx')

    cols_funds = ['cnpj', 'dtposicao', 'cnpjfundo', 'equity_stake', 'composicao',
                  'valor_calc', 'isin', 'NEW_TIPO', 'fNUMERACA.DESCRICAO',
                  'fEMISSOR.NOME_EMISSOR', 'NEW_NOME_ATIVO', 'NEW_GESTOR',
                  'NEW_GESTOR_WORD_CLOUD']

    utl.log_message('Carregando arquivo de fundos.')
    dtypes = dta.read("fundos_metadata")
    file_name = f"{xlsx_destination_path}fundos"
    funds = fhdl.load_df(file_name, file_ext, dtypes)

    validate_fund_graph_is_acyclic(funds)

    funds = funds[funds['valor_serie'] == 0][cols_funds].copy()

    cols_port = ['cnpjcpf', 'codcart', 'cnpb', 'dtposicao', 'nome', 'cnpjfundo',
                 'equity_stake', 'composicao', 'valor_calc', 'isin',
                 'NEW_TIPO', 'NEW_NOME_ATIVO', 'fEMISSOR.NOME_EMISSOR', 'NEW_GESTOR',
                 'NEW_GESTOR_WORD_CLOUD']

    utl.log_message('Carregando arquivo de carteiras.')
    dtypes = dta.read(f"carteiras_metadata")
    file_name = f"{xlsx_destination_path}carteiras"
    portfolios = fhdl.load_df(file_name, file_ext, dtypes)

    portfolios = portfolios[(portfolios['flag_rateio'] == 0) &
                            (portfolios['valor_serie'] == 0)][cols_port].copy()

    portfolios['nivel'] = 0
    portfolios['fNUMERACA.DESCRICAO'] = ''

    utl.log_message('Início processamento árvore.')
    tree_horzt = build_tree_horizontal(portfolios.copy(), funds)

    max_deep = tree_horzt['nivel'].max()
    create_column_based_on_levels(tree_horzt, 'NEW_TIPO_ATIVO_FINAL', 'NEW_TIPO', max_deep)
    create_column_based_on_levels(tree_horzt, 'NEW_NOME_ATIVO_FINAL', 'NEW_NOME_ATIVO', max_deep)
    create_column_based_on_levels(tree_horzt, 'NEW_GESTOR_WORD_CLOUD_FINAL', 'NEW_GESTOR_WORD_CLOUD', max_deep)
    create_column_based_on_levels(tree_horzt, 'fEMISSOR.NOME_EMISSOR_FINAL', 'fEMISSOR.NOME_EMISSOR', max_deep)
    create_column_based_on_levels(tree_horzt, 'PARENT_FUNDO_FINAL', 'NEW_NOME_ATIVO', max_deep-1)

    tree_horzt['SEARCH'] = (
        tree_horzt['NEW_NOME_ATIVO_FINAL']
        + ' ' + tree_horzt['NEW_GESTOR_WORD_CLOUD_FINAL']
        + ' ' + tree_horzt['fEMISSOR.NOME_EMISSOR_FINAL']
        + ' ' + tree_horzt['PARENT_FUNDO_FINAL']
    )
    utl.log_message('Fim processamento árvore.')

    utl.log_message('Salvando dados')
    file_name = f"{xlsx_destination_path}arvore_carteiras"
    fhdl.save_df(tree_horzt, file_name, 'xlsx')
    utl.log_message(f"Fim processamento árvore. Arquivo {file_name}.{file_ext}")


if __name__ == "__main__":
    main()
