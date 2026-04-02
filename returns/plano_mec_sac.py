#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  3 18:17:22 2025

@author: andrefelix
"""


import pandas as pd


def _prefix_invest_mec_sac(mec_sac: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica prefixo 'INVEST_' na coluna CODCLI do DataFrame mec_sac.

    --- CONTEXTO E MOTIVAÇÃO (GAMBIARRA DOCUMENTADA) ---

    A tabela dCadPlanoSAC (dbAux.xlsx) possui as colunas CODCLI_SAC e
    CODCLI_SAC_INVEST. A função load_dbaux() em auxiliary_loaders.py substitui
    CODCLI_SAC pelo valor de CODCLI_SAC_INVEST quando este está preenchido,
    tornando CODCLI_SAC_INVEST o identificador efetivo usado nos merges.

    Originalmente, apenas cinco carteiras tinham cálculo separado de
    investimentos no sistema YMF (020046 CONSOLID, CPFL CD_CON, 47CPFL-CD PURO,
    PIRA CD CONSOLI, PREV06 ENEL CON). Para essas carteiras, CODCLI_SAC_INVEST
    já estava preenchido com um código distinto (ex: "CESP CD", "CPFL CD").

    A partir de fevereiro de 2026, todas as carteiras passaram a ter cálculo
    separado de investimentos. Os dados do sistema YMF passaram a chegar com o
    código prefixado por 'INVEST_' (ex: 'INVEST_020046 CONSOLID'). A coluna
    CODCLI_SAC_INVEST da dCadPlanoSAC foi atualizada para refletir esse padrão
    em todas as linhas.

    O resultado é uma inconsistência temporal em mec_sac:
    - Registros anteriores a fev/2026: CODCLI sem prefixo 'INVEST_'
    - Registros a partir de fev/2026:  CODCLI com prefixo 'INVEST_'

    Como CODCLI_SAC_INVEST está preenchido em todas as linhas da dCadPlanoSAC,
    o merge de mec_sac com dcadplanosac falha para registros históricos.

    A solução correta seria uma tabela de vigência das carteiras de investimento,
    controlando desde quando cada carteira passou a ter o cálculo separado.
    Por restrições de tempo, optou-se por padronizar mec_sac aplicando o prefixo
    'INVEST_' em todos os registros que ainda não o possuem, tornando o merge
    agnóstico ao período.

    TODO: Substituir esta função por um mecanismo de vigência quando houver
    disponibilidade de tempo para refatoração adequada.

    --- FIM DO CONTEXTO ---

    Parâmetros
    ----------
    mec_sac : pd.DataFrame
        DataFrame com a coluna CODCLI já convertida para str e stripada.

    Retorno
    -------
    pd.DataFrame
        O mesmo DataFrame com CODCLI padronizado com prefixo 'INVEST_'.
        A operação é feita in-place na cópia recebida; o caller decide
        se reassigna ou não a variável original.
    """
    mask = ~mec_sac['CODCLI'].str.startswith('INVEST_')
    mec_sac.loc[mask, 'CODCLI'] = 'INVEST_' + mec_sac.loc[mask, 'CODCLI']
    return mec_sac


def compute_plan_returns_adjustment(tree_hrztl, mec_sac, dcadplanosac, port_submassa):
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
    mec_sac['CODCLI'] = mec_sac['CODCLI'].astype(str).str.strip()
    mec_sac = _prefix_invest_mec_sac(mec_sac)

    dcadplanosac['CODCLI_SAC'] = dcadplanosac['CODCLI_SAC'].astype(str).str.strip()
    dcadplanosac['CODCART'] = dcadplanosac['CODCART'].astype(str).str.strip()

    mec_sac_dcadplanosac = mec_sac.merge(
        dcadplanosac,
        how='left',
        left_on='CODCLI',
        right_on='CODCLI_SAC'
    )

    #isso eh uma gambiarra
    #como a tabela dSubmassa soh tem os CNPBs com submassa, nao agrupa por CODCART de mecSAC
    #entao, deixamos essa informacao em branco para manter o comportamento anterior
    #a implementacao de submassa
    mask = mec_sac_dcadplanosac['CODCART'].isin(port_submassa['CODCART'])
    mask &= mec_sac_dcadplanosac['DT'].isin(port_submassa['dtposicao'])
    mec_sac_dcadplanosac.loc[~mask, 'CODCART'] = ''

    mec_sac_dcadplanosac['TOTAL_PL_MEC_SAC'] = (
        mec_sac_dcadplanosac.groupby(['CNPB', 'CODCART', 'DT'])['VL_PATRLIQTOT1']
        .transform('sum')
    )

    mec_sac_dcadplanosac['RENTAB_DIA_PONDERADA_MEC_SAC'] = (
        mec_sac_dcadplanosac['VL_PATRLIQTOT1']
        / mec_sac_dcadplanosac['TOTAL_PL_MEC_SAC']
        * mec_sac_dcadplanosac['RENTAB_DIA']
        )

    mec_sac_dcadplanosac['CODCART'] = mec_sac_dcadplanosac['CODCART'].fillna('')
    mec_sac_returns_by_plan = (
        mec_sac_dcadplanosac
        .groupby(['CNPB', 'CODCART', 'DT'], as_index=False)['RENTAB_DIA_PONDERADA_MEC_SAC']
        .sum()
    )

    tree_returns_by_plan = (
        tree_hrztl
        .groupby(['cnpb', 'CODCART', 'dtposicao'], as_index=False)['contribution_rentab_ponderada']
        .sum()
    )

    plan_returns_adjust = tree_returns_by_plan.merge(
        mec_sac_returns_by_plan,
        left_on=['cnpb', 'CODCART', 'dtposicao'],
        right_on=['CNPB', 'CODCART', 'DT'],
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
