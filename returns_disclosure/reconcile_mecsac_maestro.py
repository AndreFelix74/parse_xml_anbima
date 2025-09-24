#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 23 08:54:20 2025

@author: andrefelix
"""


def reconcile_entities_ids(rentab_mecsac, tipo, api_data):
    """
    Map entity names in `rentab_mecsac` to their API IDs and update the DataFrame.

    Args:
        rentab_mecsac (pd.DataFrame): Local returns DataFrame with columns ['TIPO', 'NOME'].
        tipo (str): Entity type filter to select rows for mapping.
        api_data (pd.DataFrame): API DataFrame with columns ['nome', 'id'].

    Returns:
        None: Updates `rentab_mecsac` in place by adding or filling the 'api_id' column.
    """
    mapa = dict(zip(api_data['nome'], api_data['id']))

    mask = rentab_mecsac['TIPO'] == tipo

    rentab_mecsac.loc[mask, 'api_id'] = (
        rentab_mecsac.loc[mask, 'NOME'].map(mapa)
    )


def reconcile_monthly_returns(rentab_mecsac, api_data):
    """
    Reconcile monthly returns by merging local and API DataFrames.

    Args:
        rentab_mecsac (pd.DataFrame): Local returns DataFrame with ['MES', 'ANO'].
        api_data (pd.DataFrame): API DataFrame with ['mes', 'ano'] and return values.

    Returns:
        pd.DataFrame: Merged DataFrame with both local and API monthly returns.
    """
    return rentab_mecsac.merge(
        api_data,
        left_on=['MES', 'ANO'],
        right_on=['mes', 'ano'],
        )


def reconcile_annually_returns(rentab_mecsac, api_data):
    """
    Reconcile annual returns by merging local and API DataFrames.

    Args:
        rentab_mecsac (pd.DataFrame): Local returns DataFrame with ['ANO'].
        api_data (pd.DataFrame): API DataFrame with ['ano'] and return values.

    Returns:
        pd.DataFrame: Merged DataFrame with both local and API annual returns.
    """
    return rentab_mecsac.merge(
        api_data,
        left_on=['ANO'],
        right_on=['ano'],
        )
