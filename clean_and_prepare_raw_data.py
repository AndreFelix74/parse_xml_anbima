#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:31:03 2025

@author: andrefelix
"""


import os
import inspect
import pandas as pd
import util as utl
import data_access as dta


DEBUG = False


def print_debug(msg):
    """
    Prints the given message if debugging is enabled.

    Parameters:
    msg (str): The message to be printed.

    This function only prints the message when the global DEBUG variable is set to True.
    If DEBUG is False, the function does nothing.

    Example:
    >>> DEBUG = True
    >>> print_debug("This is a debug message.")
    This is a debug message.
    """
    if DEBUG:
        print(msg)


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
        print_debug(f"{key} harmonization rule")
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

    header_daily_values = dta.read('header_daily_values')
    types_to_exclude = dta.read('types_to_exclude')

    types_series = [key for key, value in header_daily_values.items() if value.get('serie', True)]

    harmonization_rules = dta.read('harmonization_values_rules')

    entities = ['fundos', 'carteiras']

    for entity_name in entities:
        dtypes = dta.read(f"{entity_name}_metadata")

        entity = pd.read_excel(f"{xlsx_destination_path}{entity_name}_raw.xlsx", dtype=dtypes)

        entity = entity[~entity['tipo'].isin(types_to_exclude)]

        harmonize_values(entity, harmonization_rules)

        entity['valor_serie'] = entity['valor'].where(entity['tipo'].isin(types_series), 0)
        entity['valor_calc'] = entity['valor_calc'].where(~entity['tipo'].isin(types_series), 0)

        mask = (
            (entity['valor_serie'] != 0)
            | (entity['valor_calc'] != 0)
            | (entity['tipo'] == 'partplanprev')
        )
        entity = entity[mask]

        entity.to_excel(f"{xlsx_destination_path}{entity_name}_staged.xlsx", index=False)


if __name__ == "__main__":
    main()
