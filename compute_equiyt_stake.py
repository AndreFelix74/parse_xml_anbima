#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:31:03 2025

@author: andrefelix
"""


import os
import pandas as pd
import util as utl
import data_access as dta


def compute_equity_stake(df_investor, df_invested):
    """
    Calculate the equity stake of investors based on available quotas and fund values.

    Args:
        df_investor (pd.DataFrame): DataFrame containing investor data with columns
                                    'cnpjfundo', 'qtdisponivel', and 'dtposicao'.
        df_invested (pd.DataFrame): DataFrame containing fund data with columns
                                    'cnpj', 'valor', and 'dtposicao'.

    Returns:
        pd.DataFrame: A DataFrame with the calculated equity stake for each investor.
    """
    equity_stake = pd.DataFrame(columns=['equity_stake'])

    columns = ['cnpjfundo', 'qtdisponivel', 'dtposicao']

    if not all(col in df_investor.columns for col in columns):
        return equity_stake

    cotas = df_investor[df_investor['cnpjfundo'].notnull()][columns]

    missing_cotas = cotas[~cotas['cnpjfundo'].isin(df_invested['cnpj'])]

    if len(missing_cotas) != 0:
        print(f"cnpjfundo nao encontrado: {missing_cotas['cnpjfundo'].unique()}")

    cotas['orinal_index'] = cotas.index

    columns_invested = ['cnpj', 'valor', 'dtposicao']

    equity_stake = cotas.merge(
        df_invested[df_invested['tipo'] == "quantidade"][columns_invested],
        left_on=['cnpjfundo', 'dtposicao'],
        right_on=['cnpj', 'dtposicao'],
        how='inner'
    )

    equity_stake.set_index('orinal_index', inplace=True)

    equity_stake['equity_stake'] = equity_stake['qtdisponivel'] / equity_stake['valor']

    return equity_stake


def compute_proportional_allocation(df_investor, types_to_exclude):
    """
    Compute the real state equity value for investors based on participation percentage
    and book value.

    Args:
        df_investor (pd.DataFrame): DataFrame containing investor data with columns
                                    'percpart' and 'valorcontabil'.

    Returns:
        pd.DataFrame: A DataFrame with the calculated real state equity values.
    """
    required_columns = ['percpart', 'new_valor', 'codcart']

    if not all(col in df_investor.columns for col in required_columns):
        return pd.DataFrame(columns=['valor'])

    allocation = df_investor[df_investor['tipo'] == 'partplanprev'].drop(columns=['new_valor'])
    allocation['original_index'] = allocation.index

    df_investor_filtered = df_investor[~df_investor['tipo'].isin(types_to_exclude + ['partplanprev'])]

    allocation_value = allocation.merge(
        df_investor_filtered[['codcart', 'new_valor']].dropna(subset=['new_valor']),
        on='codcart',
        how='inner'
    )

    allocation_value['percpart'] = pd.to_numeric(allocation_value['percpart'], errors='coerce')
    allocation_value['new_valor'] = pd.to_numeric(allocation_value['new_valor'], errors='coerce')

    allocation_value['new_valor'] = (
        allocation_value['percpart'] *
        allocation_value['new_valor'] / 100.0
        )

    allocation_value = allocation_value.set_index('original_index')

    return allocation_value


def harmonize_values(dtfr, harmonization_rules):
    dtfr['new_valor'] = None

    for key, value in harmonization_rules.items():
        filters = value["filters"]
        formula = value["formula"]

        filter_columns = [filter_item['column'] for filter_item in filters]
        missing_columns = [col for col in filter_columns if col not in dtfr.columns]
        
        if missing_columns:
            print(f"Warning: The following filter columns are missing in the DataFrame: {', '.join(missing_columns)}")
            continue

        query_parts = []
        for filter_item in filters:
            query_parts.append(f"{filter_item['column']} == '{filter_item['value']}'")
        query = " & ".join(query_parts)

        filtered_df = dtfr.query(query)

        for idx in filtered_df.index:
            if formula == "0":
                dtfr.at[idx, 'new_valor'] = 0
            else:
                print(formula)
                formula_value = eval(formula, {}, dtfr.loc[idx].to_dict())
                dtfr.at[idx, 'new_valor'] = formula_value

def main():
    """
    Main function for processing fund and portfolio data:
    - Reads configuration settings.
    - Loads raw fund and portfolio data from Excel files.
    - Computes equity stake and real state equity values.
    - Saves processed data back to Excel files.
    """
    config = utl.load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    dtypes = dta.read("fundos_metadata")

    funds = pd.read_excel(f"{xlsx_destination_path}fundos_raw.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(funds, funds)
    funds.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    funds.to_excel(f"{xlsx_destination_path}fundos.xlsx", index=False)

    dtypes = dta.read(f"carteiras_metadata")

    portfolios = pd.read_excel(f"{xlsx_destination_path}carteiras_raw.xlsx", dtype=dtypes)

    equity_stake = compute_equity_stake(portfolios, funds)
    portfolios.loc[equity_stake.index, 'equity_stake'] = equity_stake['equity_stake']

    harmonization_rules = dta.read('harmonization_values_rules')
    harmonize_values(portfolios, harmonization_rules)
    
    keys_to_not_allocate = dta.read('header_daily_values')
    keys_to_not_allocate = {key: value for key, value in keys_to_not_allocate.items() if not value.get('allocation', False)}

    equity_real_state = compute_proportional_allocation(portfolios, list(keys_to_not_allocate.keys()))
    portfolios.loc[equity_real_state.index, ['tipo', 'valor']] = equity_real_state[['tipo', 'valor']].values

    portfolios.to_excel(f"{xlsx_destination_path}/carteiras.xlsx", index=False)


if __name__ == "__main__":
   main()
