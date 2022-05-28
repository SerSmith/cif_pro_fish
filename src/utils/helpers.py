"""Вспомогательные функции
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
from pandas.api.types import is_numeric_dtype
from datetime import datetime, timedelta

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

def match(catch_merge, ext_merge, date, trashold, window=0):

    catch_date = catch_merge[catch_merge.catch_date == date].copy()
    ext_date = ext_merge[ext_merge.date == date].copy()

    print(catch_date.shape, ext_date.shape)
    catch_date['key'] = 0
    ext_date['key'] = 0
    result = pd.merge(catch_date, ext_date[['id_vsd','id_fish','fish','volume','unit','date','Region_Plat','key','id_ves']], on ='key').drop("key", 1)
    result['catch_upper'] = result['catch_volume'] * (1 + trashold)
    result['catch_lower'] = result['catch_volume'] * (1 - trashold)
    result['catch_upper_1000'] = result['catch_volume']/1000 * (1 + trashold)
    result['catch_lower_1000'] = result['catch_volume']/1000 * (1 - trashold)
    result_match = result[((result.catch_upper>=result.volume) & (result.catch_lower<=result.volume)) | ((result.catch_upper_1000>=result.volume) & (result.catch_lower_1000<=result.volume))][['id_ves_x','id_fish_x','fish_x','catch_date']].drop_duplicates().copy()
    result_match['match'] = 'True'
    print('catch: ',catch_date[['id_ves','id_fish','fish']].drop_duplicates().shape[0], 'match: ', result_match.shape[0])
    result_final = pd.merge(result, result_match[['id_ves_x','id_fish_x','fish_x','match']], on=['id_ves_x','id_fish_x','fish_x'], how='left')
    result_final = result_final[result_final.match.isnull()]
    print('result_final.shape with not match',result_final.shape)
    result_not_match = result_final[['id_ves_x','id_fish_x','fish_x','catch_volume','catch_date']].drop_duplicates()
    result_not_match.columns = ['id судна','id рыбы','название рыбы','масса (кг)','дата']
    return result_not_match, result_final

def find_close(result_merge, app_data, num=20):

    OUT_COLUMN_NAMES = ['id записи (id_vsd)','id судна', 'id рыбы','назавание рыбы','масса (кг)','дата']
    selected_rows = app_data['selected_rows']
    if len(selected_rows) == 0:
        result_head = pd.DataFrame([], columns=OUT_COLUMN_NAMES)
    else:
        select = selected_rows[0]
        print('I am here', select)
        result = result_merge[(result_merge.id_ves_x == select['id судна']) & (result_merge.id_fish_x == select['id рыбы'])]
        result['factor'] = abs(result['catch_volume'].astype(float) / result['volume'].astype(float) - 1)
        result_head = result.sort_values('factor', ascending=True).head(num)[['id_vsd','id_ves_y','id_fish_y','fish_y','volume','date']]
        result_head.columns = OUT_COLUMN_NAMES

    return result_head
