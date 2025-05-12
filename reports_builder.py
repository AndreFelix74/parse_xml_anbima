#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 22:50:10 2025

@author: andrefelix
"""


import os
import pandas as pd
import util as utl
import data_access as dta
from data_loader import load_mec_sac_last_day_month, load_cnpb_codcli_mapping


def calculate_invested_returns(investor):
    """
    Calculate the return (rentabilidade) of each fund based on
    the variation in price per quota (puposicao) over time.

    Args:
        investor (pd.DataFrame): DataFrame containing price data with columns:
                           'cnpjfundo', 'dtposicao', 'puposicao'.

    Returns:
        pd.DataFrame: A copy of the input DataFrame with an added 'rentabilidade' column,
                      which represents the return between subsequent positions for each fund.
    """
    required_columns = ['cnpjfundo', 'dtposicao', 'puposicao']

    utl.validate_required_columns(investor, required_columns)

    returns = investor[investor['cnpjfundo'].notnull()][required_columns].drop_duplicates().copy()
    returns.sort_values(by=['cnpjfundo', 'dtposicao'], inplace=True)
    returns['rentabilidade'] = returns.groupby('cnpjfundo')['puposicao'].pct_change()

    return returns


def get_combined_returns(funds: pd.DataFrame, portfolios: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a rentabilidade dos fundos e das carteiras, e retorna um DataFrame combinado
    com as rentabilidades por cnpjfundo e dtposicao, removendo duplicações.

    Args:
        funds (pd.DataFrame): DataFrame com dados dos fundos (inclui 'cnpjfundo',
        dtposicao', 'puposicao').
        portfolios (pd.DataFrame): DataFrame com dados das carteiras.

    Returns:
        pd.DataFrame: DataFrame consolidado com colunas ['cnpjfundo', 'dtposicao',
        'puposicao', 'rentabilidade'],
        sem duplicações.
    """
    funds_returns = calculate_invested_returns(funds)
    portfolios_returns = calculate_invested_returns(portfolios)

    combined_returns = pd.concat([funds_returns, portfolios_returns], ignore_index=True)
    combined_returns.drop_duplicates(subset=['cnpjfundo', 'dtposicao'], keep='first', inplace=True)

    return combined_returns


def reshape_estrutura_gerencial(struct_map):
    """
    Lê a planilha de estrutura gerencial e transforma do formato wide para long.
    Converte todas as colunas, exceto 'CNPJ_VEICULO', para linhas com tipo/valor.

    Returns:
        pd.DataFrame com colunas ['TIPO', 'valor', 'cnpjfundo']
    """
    struct_map = struct_map.rename(columns={"CNPJ_VEICULO": "cnpjfundo"}).copy()

    # Mantém a coluna 'cnpjfundo' fixa e transforma o resto em TIPO/valor
    struct_long = pd.melt(
        struct_map,
        id_vars=['cnpjfundo'],
        var_name='estrutura_tipo',
        value_name='estrutura_item'
    )

    return struct_long


def generate_to_be_define_struct(portfolios, attributes):
    """
    Identifica as linhas de 'portfolios' que não possuem chave correspondente em 'attributes'
    e cria um dataframe cartesiano dessas linhas com todos os 'estrutura_tipo' únicos,
    marcando 'estrutura_item' como 'A DEFINIR'.

    Args:
        portfolios (pd.DataFrame): DataFrame original contendo os dados das carteiras.
        attributes (pd.DataFrame): DataFrame contendo as estruturas (dePara).

    Returns:
        pd.DataFrame: DataFrame expandido contendo as linhas sem chave,
        duplicadas por 'estrutura_tipo', com 'estrutura_item' preenchido como 'A DEFINIR'.
    """
    attributes_keys = attributes[['cnpjfundo']].drop_duplicates()

    rows_without_match = portfolios[
        ~portfolios['cnpjfundo'].isin(attributes_keys)
    ].drop(columns=['puposicao'], errors='ignore')

    if rows_without_match.empty:
        return None

    rows_without_match['key_to_be_defined'] = 1

    struct_types_unique = attributes[['estrutura_tipo']].dropna().drop_duplicates()
    struct_types_unique['key_to_be_defined'] = 1
    struct_types_unique['estrutura_item'] = 'A DEFINIR'

    to_be_define = rows_without_match.merge(
        struct_types_unique,
        on='key_to_be_defined',
        how='inner'
    ).drop(columns=['key_to_be_defined'])

    return to_be_define


def prepare_portfolios_dataframe(portfolios, group_tot_invest):
    """
    Processes the portfolios DataFrame.

    - Selects the predefined set of columns.
    - Computes 'total_invest' as the sum of 'valor_calc' grouped by the provided group keys.

    Parameters
    ----------
    portfolios_df : pd.DataFrame
        The raw portfolios DataFrame (already loaded from Excel).
    group_tot_invest : list of str
        List of column names to group by when computing 'total_invest'.

    Returns
    -------
    pd.DataFrame
        The processed portfolios DataFrame with an additional 'total_invest' column.
    """
    cols_portfolios = [
        'cnpjcpf', 'codcart', 'dtposicao', 'nome', 'cnpb',
        'tipo', 'cnpjfundo', 'valor_calc', 'puposicao',
    ]

    portfolios = portfolios[portfolios['valor_calc'] != 0][cols_portfolios]

    portfolios['total_invest'] = (
        portfolios.groupby(group_tot_invest)['valor_calc']
        .transform('sum')
    )

    return portfolios


def compute_structure_returns(report, group_tot_struct):
    """
    Computes the structural composition and rentability report.

    - Computes total values and composition percentages within structures.
    - Calculates proportional and aggregated rentability.

    Parameters
    ----------
    report : pd.DataFrame
        Processed report DataFrame.
    attributes : pd.DataFrame
        Attributes DataFrame with structural mapping.
    group_tot_struct : list of str
        Keys to group by when computing totals.

    Returns
    -------
    pd.DataFrame
        The final report DataFrame with computed structural compositions and rentability.
    """
    report['composicao'] = report['valor_calc'] / report['total_invest']
    report['rentab_prop'] = report['composicao'] * report['rentabilidade']

    report['tot_item_estrutura'] = (
        report.groupby(group_tot_struct)['valor_calc']
        .transform('sum')
    )

    report['rentab_item_estrutura'] = (
        report.groupby(group_tot_struct)['rentab_prop']
        .transform('sum')
    )

    return report


def main():
    """
    Main function for processing fund and portfolio data:
    - Reads configuration settings.
    - Loads enriched funds and portfolios data from Excel files.
    - Loads portfolios returns
    - Loads estrutura gerencial
    - Computes returns based on cnpjfundo, dtposicao and puposicao.
    - Computes proportional aggregated returns by estrutura gerencial.
    - Computes adjusts
    - Saves processed data in Excel files.
    """
    config = utl.load_config('config.ini')

    xlsx_aux_path = config['Paths']['xlsx_aux_path']
    xlsx_aux_path = f"{os.path.dirname(utl.format_path(xlsx_aux_path))}/"

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    dtypes = dta.read("fundos_metadata")
    funds = pd.read_excel(f"{xlsx_destination_path}fundos.xlsx",
                          dtype=dtypes)

    dtypes = dta.read(f"carteiras_metadata")
    portfolios = pd.read_excel(f"{xlsx_destination_path}carteiras.xlsx",
                               dtype=dtypes)

    group_tot_invest = ['cnpjcpf', 'codcart', 'dtposicao', 'nome', 'cnpb']

    portfolios = prepare_portfolios_dataframe(portfolios, group_tot_invest)
    portfolios['dtposicao'] = portfolios['dtposicao'].astype('datetime64[s]')

    mec_sac_path = './_data/mecSAC/'
    dbaux_path = './_data/'

    portfolios_returns = load_mec_sac_last_day_month(mec_sac_path)
    cnpb_codcli_mapping = load_cnpb_codcli_mapping(dbaux_path)

    returns_cnpb = portfolios_returns[['CLCLI_CD', 'DT', 'RENTAB_MES', 'RENTAB_ANO']].merge(
        cnpb_codcli_mapping,
        left_on='CLCLI_CD',
        right_on='CODCLI_SAC',
        how='inner'
    )
    returns_cnpb.rename(columns={'DT': 'dtposicao'}, inplace=True)

    portfolios = portfolios.merge(
        returns_cnpb[['dtposicao', 'RENTAB_MES', 'RENTAB_ANO', 'cnpb', 'CODCLI_SAC']],
        on=['cnpb', 'dtposicao'],
        how='left'
    )

    funds_returns = get_combined_returns(funds, portfolios)
    funds_returns['dtposicao'] = funds_returns['dtposicao'].astype('datetime64[s]')

    report = portfolios.drop(columns=['puposicao']).merge(
        funds_returns.drop(columns=['puposicao']),
        on=['cnpjfundo', 'dtposicao'],
        how='left'
    )

    struct_map = pd.read_excel(f"{xlsx_aux_path}dePara_Estrutura.xlsx")
    attributes = reshape_estrutura_gerencial(struct_map)
    attributes.dropna(subset=['cnpjfundo'], inplace=True)
    attributes['cnpjfundo'] = (
        attributes['cnpjfundo']
        .astype(str)
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)
        .str.zfill(14)
    )

    struct_to_be_define = generate_to_be_define_struct(portfolios, attributes)

    report = report.merge(
        attributes.dropna(subset=['cnpjfundo']),
        on=['cnpjfundo'],
        how='inner'
    )

    report = pd.concat([report, struct_to_be_define], ignore_index=True)

    group_tot_struct = group_tot_invest + ['estrutura_tipo', 'estrutura_item']

    report = compute_structure_returns(report, group_tot_struct)

    sort_columns = ['dtposicao', 'cnpjcpf', 'codcart', 'cnpb', 'estrutura_tipo',
                    'estrutura_item']

    report.sort_values(by=sort_columns, inplace=True)

    report.to_excel('report.xlsx')


if __name__ == "__main__":
    main()
