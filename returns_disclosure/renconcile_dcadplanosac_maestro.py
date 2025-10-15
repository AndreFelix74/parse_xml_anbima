#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 13 22:04:55 2025

@author: andrefelix
"""


def reconcile_entities_ids(dcadplanosac, label, api_data):
    """
    Map entity names in `dcadplanosac` to their API IDs and update the DataFrame.

    Args:
        dcadplanosac (pd.DataFrame): dCadPlanoSac DataFrame with columns ['TIPO', 'NOME'].
        label (str): Entity type filter to select rows for mapping.
        api_data (pd.DataFrame): API DataFrame with columns ['nome', 'id'].

    Returns:
        None: Updates `dcadplanosac` in place by adding or filling the 'api_id' column.
    """
    mapa = {item["nome"].upper(): item["id"] for item in api_data}

    mask = dcadplanosac['TIPO'] == label

    dcadplanosac.loc[mask, 'api_id'] = (
        dcadplanosac.loc[mask, 'NOME'].str.upper().map(mapa)
    )
