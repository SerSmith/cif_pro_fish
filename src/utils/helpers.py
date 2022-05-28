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

def deduplication_db2(ext, ext2):
    d = ext.id_vsd.value_counts()>1
    ext_dup = ext[ext.id_vsd.isin(list(d[d==True].index))]

    def coalesce(l):
        if l[0]!=-1 and l[0]!='\\N':
            return l[0]
        else: return l[1]
    
    def deduplication_ext(grouping):
        #print(grouping, type(grouping))
        df = grouping.copy()
        group_label = df['id_vsd'].unique()
        df_new = pd.DataFrame()
        for column in df.columns:
            #print(coalesce(list(df[column])))
            df_new[column] = [coalesce(list(df[column]))]
        #print(df_new)
        return ({group_label:df_new})
    
    # через ext_dup.groupby('id_vsd').deduplication_ext() не работает
    df_result = pd.DataFrame()
    for j in tqdm(ext_dup.groupby('id_vsd')):
        (label, df) = j
        df_new = pd.DataFrame()
        for column in df.columns:
            df_new[column] = [coalesce(list(df[column]))]
        df_result = pd.concat([df_result, df_new])

    ext_norm = ext[~ext.id_vsd.isin(list(ext_dup.id_vsd.unique()))]
    ext = pd.concat([ext_norm, df_result])
    print('ext ',ext.shape, len(ext.id_vsd.unique()))

    d = ext2.id_vsd.value_counts()>1
    ext_dup2= ext2[ext2.id_vsd.isin(list(d[d==True].index))]
    ext_without_dup2 = ext_dup2[ext_dup2.unit=='\\N']
    ext2_norm = ext2[~ext2.id_vsd.isin(list(ext_dup2.id_vsd.unique()))]
    ext2 = pd.concat([ext2_norm, ext_without_dup2])
    print('ext2 ',ext2.shape, len(ext2.id_vsd.unique()))

    return ext, ext2
