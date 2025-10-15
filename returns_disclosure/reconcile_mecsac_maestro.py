#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 23 08:54:20 2025

@author: andrefelix
"""


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
        api_data.add_suffix('_mensal'),
        left_on=['api_id', 'MES', 'ANO'],
        right_on=['planoId_mensal', 'mes_mensal', 'ano_mensal'],
        how='left',
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
        api_data.add_suffix('_anual'),
        left_on=['api_id', 'ANO'],
        right_on=['planoId_anual', 'ano_anual'],
        how='left',
        )
