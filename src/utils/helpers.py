"""Вспомогательные функции
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
from pandas.api.types import is_numeric_dtype

def check_intersection(table1, table2, key1, key2, round_to=None):
    """Поиск пересечений по потенциальным ключам

    Args:
        table1 (_type_): _description_
        table2 (_type_): _description_
        key1 (_type_): _description_
        key2 (_type_): _description_

    Returns:
        _type_: _description_
    """
    if round_to is not None:
        if is_numeric_dtype(table1[key1]) and is_numeric_dtype(table2[key2]):
            table1[key1] = table1[key1].round(round_to)
            table2[key2] = table2[key2].round(round_to)
    unique_keys1 = set(table1[key1])
    unique_keys2 = set(table2[key2])

    intersect = unique_keys1 & unique_keys2
    difference = unique_keys1.symmetric_difference(unique_keys2)
    all_keys = unique_keys1 | unique_keys2
    n_intersect = len(intersect)
    n_different = len(difference)
    n_min = min(len(unique_keys1), len(unique_keys2))
    n_max = max(len(unique_keys1), len(unique_keys2))
    n_all = len(all_keys)
    intersection_quality = n_intersect / n_all
    return [n_intersect, n_different, n_min, n_max, n_all, intersection_quality]


def get_potential_keys(table1, table2, top=None, round_to=None):
    """Смотрит совпадение уникальных значений между колонками двух таблиц

    Args:
        table1 (_type_): _description_
        table2 (_type_): _description_
        top (_type_, optional): _description_. Defaults to None.
        round_to (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    keys_quality_list = []
    for col1 in tqdm(table1.columns):
        for col2 in tqdm(table2.columns):
            result = check_intersection(table1, table2, col1, col2, round_to=round_to)
            result = [col1, col2] + result
            keys_quality_list.append(result)
    quality_df =  pd.DataFrame(data=keys_quality_list,
                               columns=['key1', 'key2', 'n_intersect', 'n_different',
                                        'n_min', 'n_max', 'n_all', 'intersection_quality'])
    quality_df = quality_df.sort_values(by='intersection_quality', ascending=False)
    if top is not None:
        quality_df = quality_df.head(top)
    return quality_df


def get_date_features(dt_series):
    """Создает из колонки datetime[ns] фичи даты

    Args:
        dt_series (_type_): _description_

    Returns:
        _type_: _description_
    """
    result = pd.DataFrame(columns=['weekday', 'day', 'month'],
                          index=dt_series.index,
                          data=np.nan)
    result['weekday'] = dt_series.dt.weekday
    result['day'] = dt_series.dt.day
    result['month'] = dt_series.dt.month
    return result


def get_good_unique_for_ohe(data, column, threshold=.05):
    """выбирает только те значения признака, которые встречаются чаще чем в threshold * 100% случаях

    Args:
        data (_type_): _description_
        column (_type_): _description_
        threshold (float, optional): _description_. Defaults to .05.

    Returns:
        _type_: _description_
    """
    val_counts = data[column].value_counts()
    max_count = val_counts.quantile(.95)
    val_frac = val_counts / max_count
    good_cols = val_frac[val_frac >= threshold].index
    return list(good_cols)


def get_good_volume_for_ohe(data, column, value_column, threshold=.05):
    """выбирает только те значения признака, которые встречаются чаще чем в threshold * 100% случаях

    Args:
        data (_type_): _description_
        column (_type_): _description_
        threshold (float, optional): _description_. Defaults to .05.

    Returns:
        _type_: _description_
    """
    sum_value = data.groupby(column)[value_column].sum()
    max_value = sum_value.quantile(.95)
    val_frac = sum_value / max_value
    good_cols = val_frac[val_frac >= threshold].index
    return list(good_cols)
