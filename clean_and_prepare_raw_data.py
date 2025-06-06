#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:31:03 2025

@author: andrefelix
"""


import inspect
import pandas as pd


def harmonize_values(dtfr, harmonization_rules):
    """
    Harmonize values in a DataFrame based on a set of rules.

    This function applies transformations to a specified column (`valor`) in a pandas DataFrame
    using a set of harmonization rules defined in a dictionary. Each rule specifies filters to
    select rows and a formula to compute the new value for the `valor` column.

    Parameters:
    ----------
    dtfr : pandas.DataFrame
        The input DataFrame containing data to be harmonized.
        Must include columns referenced in the filters and formulas.

    harmonization_rules : dict
        A dictionary where each key represents a harmonization rule name
        and each value is a dictionary containing:
        - "filters": A list of dictionaries, where each dictionary specifies:
            - "column" (str): The name of the column to filter.
            - "value" (str or any): The value to match for filtering.
        - "formula": A string representing a formula to evaluate,
          a list of columns to sum, or a constant value.

    Returns:
    -------
    pandas.DataFrame
        The DataFrame with the `valor` column harmonized according to the rules.

    Formula Handling:
    -----------------
    - If `formula` is a string: It is evaluated as a pandas expression using the `eval` function.
    - If `formula` is a list: The specified columns are summed across rows.
    - If `formula` is a constant: The value is directly assigned to the `valor` column.

    Notes:
    ------
    - Rows not matching any rule will retain `None` or their original value in the `valor` column.
    - Warnings are printed if any filter references columns missing from the DataFrame.

    Example:
    --------
    >>> import pandas as pd
    >>> data = {'tipo': ['caixa', 'cotas', 'caixa'], 'saldo': [100, 200, 300]}
    >>> df = pd.DataFrame(data)
    >>> rules = {
    ...     "CAIXA": {"filters": [{"column": "tipo", "value": "caixa"}], "formula": "saldo * 1.1"},
    ...     "COTAS": {"filters": [{"column": "tipo", "value": "cotas"}], "formula": "saldo * 0.9"}
    ... }
    >>> harmonized_df = harmonize_values(df, rules)
    >>> print(harmonized_df)
         tipo  saldo  valor
    0   caixa    100  110.0
    1   cotas    200  180.0
    2   caixa    300  330.0
    """
    dtfr['valor_calc'] = None

    for key, value in harmonization_rules.items():
        try:
            filters = value["filters"]
            formula = value["formula"]

            filter_columns = [filter_item['column'] for filter_item in filters]

            validate_required_columns(dtfr, filter_columns)

            mask = pd.Series(True, index=dtfr.index)
            for filter_item in filters:
                column, filter_value = filter_item['column'], filter_item['value']
                mask &= (dtfr[column] == filter_value)

            if sum(mask) == 0:
                continue

            if isinstance(formula, str):
                formula_expr = formula
                dtfr.loc[mask, 'valor_calc'] = dtfr.loc[mask].eval(formula_expr)
            elif isinstance(formula, list):
                dtfr.loc[mask, 'valor_calc'] = dtfr.loc[mask, formula].sum(axis=1)
            else:
                dtfr.loc[mask, 'valor_calc'] = formula
        except Exception as excpt:
            raise ValueError(
                f"[harmonize_values] Erro ao aplicar fórmula na regra '{key}': {excpt}"
            ) from excpt


def validate_required_columns(dtfrm: pd.DataFrame, required_columns: list):
    """
    Validates that all required columns are present in the given DataFrame.
    Automatically identifies the name of the calling function to include in error messages.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        required_columns (list): A list of column names that must be present.

    Raises:
        ValueError: If one or more required columns are missing.
    """
    missing_columns = [col for col in required_columns if col not in dtfrm.columns]
    if missing_columns:
        caller_name = inspect.stack()[1].function
        raise ValueError(f"[{caller_name}] Missing required columns: {', '.join(missing_columns)}")


def clean_data(raw, dtypes, types_to_exclude, types_series, harmonization_rules):
    """
    Aplica limpeza e harmonização aos dados de uma entidade (fundos ou carteiras).

    Args:
        df (pd.DataFrame): DataFrame bruto.
        types_to_exclude (list): Lista de tipos a descartar.
        harmonization_rules (dict): Regras de harmonização para cálculo de valor.
        types_series (list): Tipos considerados como séries diárias.

    Returns:
        pd.DataFrame: DataFrame limpo e harmonizado.
    """
    valid_dtypes = {col: dtype for col, dtype in dtypes.items() if col in raw.columns}

    raw = raw.astype(valid_dtypes, errors='raise')

    raw = raw[~raw['tipo'].isin(types_to_exclude)]

    harmonize_values(raw, harmonization_rules)

    raw['valor_serie'] = raw['valor'].where(raw['tipo'].isin(types_series), 0)
    raw['valor_calc'] = raw['valor_calc'].where(~raw['tipo'].isin(types_series), 0)

    mask = (
        (raw['valor_serie'] != 0)
        | (raw['valor_calc'] != 0)
        | (raw['tipo'] == 'partplanprev')
    )

    return raw[mask].copy()
