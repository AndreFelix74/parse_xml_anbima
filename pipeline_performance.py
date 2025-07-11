#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  8 16:51:15 2025

@author: andrefelix
"""


import os
import locale
from logger import log_timing

import auxiliary_loaders as aux_loader
import data_access as dta
import util as utl
from file_handler import save_df


def load_config():
    config = utl.load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    data_aux_path = config['Paths']['data_aux_path']
    data_aux_path = f"{os.path.dirname(utl.format_path(data_aux_path))}/"

    mec_sac_path = config['Paths']['mec_sac_path']
    mec_sac_path = f"{os.path.dirname(utl.format_path(mec_sac_path))}/"

    performance_path = config['Paths']['performance_path']
    performance_path = f"{os.path.dirname(utl.format_path(performance_path))}/"

    return [xlsx_destination_path, data_aux_path, mec_sac_path, performance_path]


def run_pipeline():
    locale.setlocale(locale.LC_ALL, '')

    (
        xlsx_destination_path,
        data_aux_path,
        mec_sac_path,
        performance_path
    ) = load_config()

    plano_de_para = dta.read('planos_desempenho_renaming')
    
    with log_timing('plans_returns', 'load_dcadplanosac'):
        dcadplanosac = aux_loader.load_dcadplanosac(data_aux_path)

    with log_timing('performance', 'load_struct'):
        struct = aux_loader.load_performance_struct(data_aux_path)

    struct['PERFIL_BASE'] = struct['PERFIL_BASE'].astype(str).str.strip().str.upper()

    with log_timing('performance', 'load_mec_sac'):
        mec_sac = aux_loader.load_mec_sac_last_day_month(mec_sac_path)

    months_ptbr = {
        'January': 'Janeiro', 'February': 'Fevereiro', 'March': 'Março',
        'April': 'Abril', 'May': 'Maio', 'June': 'Junho',
        'July': 'Julho', 'August': 'Agosto', 'September': 'Setembro',
        'October': 'Outubro', 'November': 'Novembro', 'December': 'Dezembro'
    }

    mec_sac['MES_ANO'] = mec_sac['DT'].dt.strftime('%B-%Y')
    mec_sac['MES_ANO'] = mec_sac['MES_ANO'].replace(months_ptbr, regex=True)

    with log_timing('performance', 'load_performance'):
        performance = aux_loader.load_performance(performance_path)

    performance['TIPO_PLANO'] = performance['PLANO'].str.split('-').str[1].fillna('').str.strip()

    mask_rochoprev = performance['PLANO'] == 'ROCHOPREV'
    performance.loc[mask_rochoprev, 'TIPO_PLANO'] = 'CV'
    
    mask_cd = performance['TIPO_PLANO'].isin(['', 'AGRESSIVO', 'MODERADO', 'CONSERVADOR'])
    performance.loc[mask_cd, 'TIPO_PLANO'] = 'CD'

    performance = performance.merge(
        struct,
        how='left',
        on='PERFIL_BASE',
        suffixes=('', '_estr')
    )

    performance = performance[performance['TIPO_PERFIL_BASE'] != 'A']

    mec_sac_dcadplanosac = mec_sac.merge(
        dcadplanosac,
        how='left',
        left_on='CODCLI',
        right_on='CODCLI_SAC'
    )

    mec_sac_dcadplanosac['total_pl'] = (
        mec_sac_dcadplanosac.groupby(['NOME_PLANO_KEY_DESEMPENHO', 'MES_ANO'])['VL_PATRLIQTOT1']
        .transform('sum')
    )

    mec_sac_dcadplanosac['RENTAB_MES_PONDERADA'] = (
        mec_sac_dcadplanosac['VL_PATRLIQTOT1']
        / mec_sac_dcadplanosac['total_pl']
        * mec_sac_dcadplanosac['RENTAB_MES']
        )
    
    mec_sac_returns_by_plan = (
        mec_sac_dcadplanosac
        .groupby(['NOME_PLANO_KEY_DESEMPENHO', 'MES_ANO'], as_index=False)['RENTAB_MES_PONDERADA']
        .sum()
        )

    performance['total_pl'] = (
        performance.groupby(['PLANO', 'DATA'])['PL']
        .transform('sum')
    )

    performance['RENTAB_MES_PONDERADA_DESEMPENHO'] = (
        performance['PL']
        / performance['total_pl']
        * performance['RETORNO_MES']
        )

    performance_by_plan = (
        performance
        .groupby(['PLANO', 'DATA'], as_index=False)['RENTAB_MES_PONDERADA_DESEMPENHO']
        .sum()
    )

    performance_by_plan['NEW_PLANO'] = (
        performance_by_plan['PLANO'].map(plano_de_para).fillna(performance['PLANO'])
        )

    performance_returns_adjust = performance_by_plan.merge(
        mec_sac_returns_by_plan,
        left_on=['NEW_PLANO', 'DATA'],
        right_on=['NOME_PLANO_KEY_DESEMPENHO', 'MES_ANO'],
        how='left'
    )

    performance_returns_adjust['ajuste_rentab'] = (
        performance_returns_adjust['RENTAB_MES_PONDERADA_DESEMPENHO']
        - performance_returns_adjust['RENTAB_MES_PONDERADA']
        )

    save_df(performance_returns_adjust, f"{xlsx_destination_path}ajuste_desempenho", 'csv')
    

if __name__ == "__main__":
    with log_timing('full', 'all_process'):
        run_pipeline()
