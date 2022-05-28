import json
import pickle
import pandas as pd
from typing import  Optional
from tqdm import tqdm
import utils.helpers as helpers

NEEDED_COLS_PATH = './models/needed_columns.json'

class prepare_models:
    def __init__(self, models_folder) -> None:
        all_models = p
        pass


    def get_susp_table(self) ->pd.DataFrame:
        """Метод, возвращаюший отсортированный спсиок полдозрительных кейсов

        Returns:
            pd.DataFrame: Датафрейм с колонками: ves_id, date, forecast, trashhold
        """        
        pass


# def add_warning_type(tab: pd.DataFrame, trashhold: Optional[float]=None) -> pd.Series:
#     """Возвращает тип опасности "Warning", 'Critical", "None"

#     Args:
#         tab (pd.DataFrame): _description_
#         trashhold (Optional[float]): _description_

#     Returns:
#         pd.Series: _description_
#     """

# class simple_model:
#     def __init__(self, path_to_model='./models/simple_model.pickle') -> None:
#         self.simple_model = pd.read_pickle(path_to_model)
#         self.susp_table = pd.DataFrame()

#     def get_susp_table(self, catch, product, threshold=.25) ->pd.DataFrame:
#         """Метод, возвращаюший отсортированный спсиок полдозрительных кейсов

#         Returns:
#             pd.DataFrame: Датафрейм с колонками: ves_id, date, forecast, trashhold
#         """
#         merged = merge_catch_product_simple(catch, product)
#         merged['efficiency_coef'] = merged['catch_volume'] / merged['prod_volume']
#         merged = merged.merge(self.simple_model, on=['id_ves'], how='left')
#         mean_coef = self.simple_model['ves_efficiency'].mean()
#         merged['efficiency_coef'] = merged['efficiency_coef'].fillna(mean_coef)
#         merged['ves_efficiency'] = merged['ves_efficiency'].fillna(mean_coef)
#         merged['coefs_ratio'] = (merged['ves_efficiency'] - merged['efficiency_coef']) / merged['ves_efficiency']
#         merged['difference'] = abs(merged['coefs_ratio']) - threshold
#         result = merged.sort_values(by='difference', ascending=False)
#         result = result.rename({'difference': 'forecast'})
#         result['trashhold'] = threshold
#         self.susp_table = result[['ves_id, date, forecast, trashhold']].copy()
#         return self.susp_table


def prepare_catch(data,
                  key_cols=None,
                  needed_cols_path=None,
                  verbose=False) ->pd.DataFrame:
    """Считает аггрегации по catch и создает ohe признаки

    Args:
        catch (_type_): _description_
    """
    if key_cols is None:
        key_cols=['id_ves', 'date']
    if needed_cols_path is None:
        needed_cols_path = NEEDED_COLS_PATH

    with open(needed_cols_path) as cols_file:
        needed_cols = json.load(cols_file)['catch']
    needed_cols = [col for col in needed_cols if col not in ['id_ves', 'date']]

    data = data.copy()

    data['date'] = pd.to_datetime(data['date'])

    id_fish_ohe = pd.get_dummies(data['id_fish'])
    fish_columns = [f'{col}_fish' for col in id_fish_ohe.columns]
    id_fish_ohe.columns = fish_columns

    id_regime_ohe = pd.get_dummies(data['id_regime'])
    regime_columns = [f'{col}_regime' for col in id_regime_ohe.columns]
    id_regime_ohe.columns = regime_columns

    id_region_ohe = pd.get_dummies(data['id_region'])
    regione_columns = [f'{col}_region' for col in id_region_ohe.columns]
    id_region_ohe.columns = regione_columns

    for col in fish_columns:
        id_fish_ohe[col] *= data['catch_volume']
    
    aggregated = data[key_cols].join(id_fish_ohe)\
        .join(id_regime_ohe)\
            .join(id_region_ohe)

    aggregated = aggregated.groupby(key_cols).sum()

    cols_to_add = [col for col in needed_cols
                  if
                  col not in aggregated.columns]

    if len(cols_to_add) != 0:
        if verbose:
            print(f'В product отсутствовали колонки {cols_to_add}')
        df_to_add = pd.DataFrame(
            index=aggregated.index,
            columns=cols_to_add,
            data=0
            )
        aggregated = aggregated.join(df_to_add)

    return aggregated.reset_index()


def prepare_product(data,
                    key_cols=None,
                    needed_cols_path=None,
                    verbose=False
                    ) ->pd.DataFrame:
    """Считает аггрегации по product и создает ohe признаки

    Args:
        product (_type_): _description_
    """

    if key_cols is None:
        key_cols=['id_ves', 'date']
    if needed_cols_path is None:
        needed_cols_path = NEEDED_COLS_PATH

    with open(needed_cols_path) as cols_file:
        needed_cols = json.load(cols_file)['product']
    needed_cols = [col for col in needed_cols if col not in ['id_ves', 'date']]

    data = data.copy()
    data['date'] = pd.to_datetime(data['date'])

    # создаем новую фичу для создания ohe
    data['id_prod_designate_type'] = data['id_prod_designate'].astype(str) + \
        '_' + data['id_prod_type'].astype(str)

    # какой продукт для какого назначения вылавливается и сколько
    id_prod_designate_type_ohe = pd.get_dummies(data['id_prod_designate_type'], drop_first=False)

    for col in tqdm(id_prod_designate_type_ohe.columns):
        id_prod_designate_type_ohe[col] *= data['prod_volume']

    aggregated = data[key_cols].join(id_prod_designate_type_ohe)
    aggregated = aggregated.groupby(key_cols).sum()
    aggregated = aggregated.reset_index()
    
    cols_to_add = [col for col in needed_cols
                  if
                  col not in aggregated.columns]

    if len(cols_to_add) != 0:
        if verbose:
            print(f'В product отсутствовали колонки {cols_to_add}')
        df_to_add = pd.DataFrame(
            index=aggregated.index,
            columns=cols_to_add,
            data=0
            )
        aggregated = aggregated.join(df_to_add)

    return aggregated


def merge_prepared_catch_product(catch_prepared, product_prepared, key_cols=None) ->pd.DataFrame:
    """Мерджит подготовленные catch и product

    Args:
        catch_prepared (_type_): _description_
        product_prepared (_type_): _description_
    """
    if key_cols is None:
        key_cols=['id_ves', 'date']

    merged = catch_prepared.merge(product_prepared, on=['id_ves', 'date'], how='inner')
    dt_merged = helpers.get_date_features(merged['date'])
    merged = merged.join(dt_merged)
    return merged.reset_index(drop=True)


def prepare_data_for_model(catch,
                          product,
                          key_cols=None,
                          needed_cols_path=None,
                          verbose=False) ->pd.DataFrame:
    """Собирает данные для обучения и использования моделей прогноза
    аномалий в первой базе

    Args:
        catch (_type_): _description_
        product (_type_): _description_
        key_cols (_type_, optional): _description_. Defaults to None.
        needed_cols_path (_type_, optional): _description_. Defaults to None.

    Returns:
        pd.DataFrame: _description_
    """
    catch_prepared = prepare_catch(catch,
                                   key_cols=key_cols,
                                   needed_cols_path=needed_cols_path,
                                   verbose=verbose
                                   )
    product_prepared = prepare_product(product,
                                       key_cols=key_cols,
                                       needed_cols_path=needed_cols_path,
                                       verbose=verbose
                                       )
    result = merge_prepared_catch_product(catch_prepared, product_prepared, key_cols=key_cols)
    return result
