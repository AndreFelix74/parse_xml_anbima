#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 18 10:26:59 2025

@author: andrefelix
"""


import os
import pandas as pd
import util as utl
import data_access as dta


def compute_returns_from_puposicao(investor: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the return (percentage change) based on the variation in PUP (price per quota)
    for both funds and portfolios, grouped by 'isin' and ordered by 'dtposicao'.

    Args:
        funds (pd.DataFrame): DataFrame containing the columns:
            ['isin', 'dtposicao','puposicao'].
        portfolios (pd.DataFrame): DataFrame containing the columns:
            ['isin', 'dtposicao', 'puposicao'].

    Returns:
        pd.DataFrame: DataFrame with columns ['isin', 'dtposicao', 'rentab'],
                      where 'rentab' represents the percentage change in 'puposicao'
                      over time for each 'isin'.
    """
    group_cols = ['cnpjfundo', 'dtposicao', 'puposicao']

    isin_returns = investor[investor['cnpjfundo'].notnull()][group_cols].drop_duplicates()

    isin_returns.sort_values(by=['cnpjfundo', 'dtposicao'], inplace=True)
    isin_returns['rentab'] = isin_returns.groupby('cnpjfundo')['puposicao'].pct_change()

    return isin_returns[['cnpjfundo', 'dtposicao', 'rentab']]


def main():
    """
    Main function for processing fund and portfolio data:
    - Reads configuration settings.
    - Loads funds and portfolios data from Excel files.
    - Computes returns
    - Saves processed data back to Excel files.
    """
    config = utl.load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    entities = ['fundos', 'carteiras']

    results = []

    for entity_name in entities:
        dtypes = dta.read(f"{entity_name}_metadata")

        entity = pd.read_excel(f"{xlsx_destination_path}{entity_name}.xlsx", dtype=dtypes)

        cnpjfundo_returns = compute_returns_from_puposicao(entity)

        results.append(cnpjfundo_returns)

    result = pd.concat(results, ignore_index=True).drop_duplicates(subset=['cnpjfundo', 'dtposicao'])
    result['dtposicao'] = result['dtposicao'].astype('datetime64[s]')

    result.sort_values(by=['cnpjfundo', 'dtposicao'], inplace=True)
    result.to_csv(f"{xlsx_destination_path}funds_returns_by_puposicao.csv",
                  index=False,
                 )


if __name__ == "__main__":
    main()
