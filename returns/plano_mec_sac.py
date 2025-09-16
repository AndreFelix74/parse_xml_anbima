#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  3 18:17:22 2025

@author: andrefelix
"""


import pandas as pd


def compute_plan_returns_adjustment(tree_hrztl, mec_sac, dcadplanosac):
    """
    Computes the difference between official monthly plan returns (from mec_sac)
    and the aggregated portfolio returns from the tree structure.

    This function performs the following steps:
    - Converts and aligns date and client identifiers.
    - Merges dcadplanosac with mec_sac to compute weighted monthly returns.
    - Aggregates both mec_sac and tree returns by plan and date.
    - Computes the return adjustment as the difference between the two sources.

    Parameters:
    ----------
    tree_hrztl : pd.DataFrame
        DataFrame representing the horizontal tree of fund compositions with
            'rentab_ponderada'.

    mec_sac : pd.DataFrame
        Official monthly return data from MEC/SAC system. Must include columns
            'CODCLI', 'DT', 'RENTAB_MES'.

    dcadplanosac : pd.DataFrame
        DataFrame with plan allocations per client. Must include 'CODCLI_SAC'
            and 'VL_PATRLIQTOT1'.

    Returns:
    -------
    list of pd.DataFrame
        [mec_sac_returns_by_plan, tree_returns_by_plan, plan_returns_adjust],
        where the last DataFrame
        includes the adjustment column 'rentab_ajuste' (mec_sac - tree).
    """
    mec_sac['DT'] = pd.to_datetime(mec_sac['DT']).dt.strftime('%Y%m%d')

    dcadplanosac['CODCLI_SAC'] = dcadplanosac['CODCLI_SAC'].astype(str).str.strip()
    mec_sac['CODCLI'] = mec_sac['CODCLI'].astype(str).str.strip()

    mec_sac_dcadplanosac = mec_sac.merge(
        dcadplanosac,
        how='left',
        left_on='CODCLI',
        right_on='CODCLI_SAC'
    )

    mec_sac_dcadplanosac['TOTAL_PL_MEC_SAC'] = (
        mec_sac_dcadplanosac.groupby(['CNPB', 'DT'])['VL_PATRLIQTOT1']
        .transform('sum')
    )

    mec_sac_dcadplanosac['RENTAB_DIA_PONDERADA_MEC_SAC'] = (
        mec_sac_dcadplanosac['VL_PATRLIQTOT1']
        / mec_sac_dcadplanosac['TOTAL_PL_MEC_SAC']
        * mec_sac_dcadplanosac['RENTAB_DIA']
        )

    mec_sac_returns_by_plan = (
        mec_sac_dcadplanosac
        .groupby(['CNPB', 'DT'], as_index=False)['RENTAB_DIA_PONDERADA_MEC_SAC']
        .sum()
    )

    tree_returns_by_plan = (
        tree_hrztl
        .groupby(['cnpb', 'dtposicao'], as_index=False)['contribution_rentab_ponderada']
        .sum()
    )

    plan_returns_adjust = tree_returns_by_plan.merge(
        mec_sac_returns_by_plan,
        left_on=['cnpb', 'dtposicao'],
        right_on=['CNPB', 'DT'],
        how='left'
    )

    plan_returns_adjust['contribution_ajuste_rentab'] = (
        plan_returns_adjust['RENTAB_DIA_PONDERADA_MEC_SAC']
        - plan_returns_adjust['contribution_rentab_ponderada']
        )

    plan_returns_adjust['contribution_ajuste_rentab_fator'] = (
        plan_returns_adjust['RENTAB_DIA_PONDERADA_MEC_SAC']
        / plan_returns_adjust['contribution_rentab_ponderada']
        )

    return [mec_sac_returns_by_plan, tree_returns_by_plan, plan_returns_adjust]
