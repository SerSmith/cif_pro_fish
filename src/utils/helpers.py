"""Вспомогательные функции
"""

import pandas as pd
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
    all = unique_keys1 | unique_keys2
    n_intersect = len(intersect)
    n_different = len(difference)
    n_min = min(len(unique_keys1), len(unique_keys2))
    n_max = max(len(unique_keys1), len(unique_keys2))
    n_all = len(all)
    intersection_quality = n_intersect / n_all
    return [n_intersect, n_different, n_min, n_max, n_all, intersection_quality]


def get_potential_keys(table1, table2, top=None, round_to=None):
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
