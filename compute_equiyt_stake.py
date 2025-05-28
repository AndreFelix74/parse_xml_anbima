#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 15 10:42:26 2025

@author: andrefelix
"""


import os
import inspect
import pandas as pd
import util as utl
import data_access as dta
import file_handler as fhdl


def validate_required_columns(df: pd.DataFrame, required_columns: list):
    """
    Validates that all required columns are present in the given DataFrame.
    Automatically identifies the name of the calling function to include in error messages.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        required_columns (list): A list of column names that must be present.

    Raises:
        ValueError: If one or more required columns are missing.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        caller_name = inspect.stack()[1].function
        raise ValueError(f"[{caller_name}] Missing required columns: {', '.join(missing_columns)}")


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

    required_columns = group_keys + ['valor_calc', 'tipo']
    validate_required_columns(investor, required_columns)

    composition = investor[
        (~investor['tipo'].isin(types_to_exclude + ['partplanprev'])) &
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


def check_puposicao(investor_holdings, invested):
    """
    Compares the 'puposicao' field in investor holdings with the 'valor' field 
    from invested data where 'tipo' is 'valorcota', for matching fund CNPJs and dates.

    Parameters:
    ----------
    investor_holdings : pandas.DataFrame
        DataFrame containing investor fund holdings with at least the columns:
        'cnpjfundo', 'dtposicao', and 'puposicao'.

    invested : pandas.DataFrame
        DataFrame containing invested values with at least the columns:
        'cnpj', 'valor', 'dtposicao', and 'tipo'. Only rows where 'tipo' == 'valorcota' 
        are used for comparison.

    Returns:
    -------
    pandas.DataFrame
        A merged DataFrame including a boolean column 'puposicao_igual_valor' that 
        indicates whether 'puposicao' and 'valor' are equal for each matched row.
    """
    columns = ['cnpjfundo', 'dtposicao', 'puposicao']
    validate_required_columns(investor_holdings, columns)

    investor_holdings['original_index'] = investor_holdings.index

    cols_invested = ['cnpj', 'valor', 'dtposicao']

    compare_puposicao = investor_holdings.merge(
        invested[invested['tipo'] == 'valorcota'][cols_invested],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner'
    )

    compare_puposicao.set_index('original_index', inplace=True)

    decimal_places = 8

    mask_diff = (
        round(compare_puposicao['puposicao'], decimal_places)
        != round(compare_puposicao['valor'], decimal_places)
    )

    return compare_puposicao.loc[mask_diff]


def compute_equity_stake(investor_holdings, invested):
    """
    Calculate the equity stake of investors based on available quotas and fund values.

    Args:
        investor_holdings (pd.DataFrame): DataFrame containing investor positions,
            with required columns: 'cnpjfundo', 'qtdisponivel', and 'dtposicao'.
        invested (pd.DataFrame): DataFrame containing fund value data,
            with required columns: 'cnpj', 'valor', 'dtposicao' and a 'tipo' column
            (must be equal to 'quantidade' for inclusion).

    Returns:
        pd.DataFrame: A DataFrame with the calculated 'equity_stake' per investor position,
            indexed by the original investor_holdings index.
    """
    columns = ['cnpjfundo', 'qtdisponivel', 'dtposicao']

    validate_required_columns(investor_holdings, columns)

    investor_holdings['original_index'] = investor_holdings.index

    columns_invested = ['cnpj', 'valor', 'dtposicao']

    equity_stake = investor_holdings.merge(
        invested[invested['tipo'] == 'quantidade'][columns_invested],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner'
    )

    equity_stake.set_index('original_index', inplace=True)

    equity_stake['equity_stake'] = equity_stake['qtdisponivel'] / equity_stake['valor']

    return equity_stake


def main():
    """
    Main function for processing fund and portfolio data:
    - Reads configuration settings.
    - Loads raw fund and portfolio data from Excel files.
    - Computes equity stake
    - Saves processed data back to Excel files.
    """
    config = utl.load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"
    file_ext = config['Paths'].get('destination_file_extension', 'xlsx')

    header_daily_values = dta.read('header_daily_values')
    types_series = [key for key, value in header_daily_values.items() if value.get('serie', True)]

    entities = [
        {
            'name': 'fundos',
            'group_keys': ['cnpj']
        },
        {
            'name': 'carteiras',
            'group_keys': ['cnpjcpf', 'codcart', 'dtposicao', 'nome', 'cnpb']
        }
    ]

    investor_holdings_cols = ['cnpjfundo', 'qtdisponivel', 'dtposicao', 'isin',
                              'NOME_ATIVO', 'puposicao']

    for entity_cfg in entities:
        entity_name = entity_cfg['name']
        group_keys = entity_cfg['group_keys']
        utl.log_message(f"Início processamento {entity_name}.")

        dtypes = dta.read(f"{entity_name}_metadata")

        file_name = f"{xlsx_destination_path}{entity_name}_enriched"
        entity = fhdl.load_df(file_name, file_ext, dtypes)

        if entity_name == 'fundos':
            invested = entity.copy()

        investor_holdings = entity[entity['cnpjfundo'].notnull()][investor_holdings_cols].copy()

        divergent_puposicao = check_puposicao(investor_holdings, invested)

        if not divergent_puposicao.empty:
            divergent_file = f"{xlsx_destination_path}{entity_name}_puposicao_divergente"
            utl.log_message(
                f"{len(divergent_puposicao)} registros com puposicao divergente. "
                f"Verifique o arquivo {divergent_file}.xlsx.",
                'warn'
            )
            fhdl.save_df(divergent_puposicao, divergent_file, 'xlsx')

        equity_stake = compute_equity_stake(investor_holdings, invested)
        entity.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

        unmatched_idx = investor_holdings.index.difference(equity_stake.index)
        if not unmatched_idx.empty:
            missing_holdings = investor_holdings.loc[unmatched_idx].drop(columns='qtdisponivel').drop_duplicates()
            missing_holdings_file = f"{xlsx_destination_path}{entity_name}_fundos_sem_xml"
            utl.log_message(f"{len(missing_holdings)} cnpjfundo não encontrados, que afetam {len(unmatched_idx)} posições. "
                            f"Verifique o arquivo {missing_holdings_file}.xlsx",
                            'warn')
            fhdl.save_df(missing_holdings, missing_holdings_file, 'xlsx')

        composition = compute_composition(entity, group_keys, types_series)
        entity.loc[composition.index, 'composicao'] = composition['composicao']

        file_name = f"{xlsx_destination_path}{entity_name}"
        fhdl.save_df(entity, file_name, 'xlsx')
        utl.log_message(f"Fim processamento {entity_name}. Arquivo {file_name}.{file_ext}")


if __name__ == "__main__":
    main()
