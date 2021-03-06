import pandas as pd
import os
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode, JsCode

def load_db1(path_to_file):
    catch = pd.read_csv(os.path.join(path_to_file, 'catch.csv'))
    product = pd.read_csv(os.path.join(path_to_file, 'product.csv'))
    ref_fish = pd.read_csv(os.path.join(path_to_file, 'ref', 'fish.csv'), sep=';')
    prod_type = pd.read_csv(os.path.join(path_to_file, 'ref', 'prod_type.csv'), sep=';')
    db1 = pd.merge(catch,ref_fish, on='id_fish', how='left' )
    db1['catch_date'] = pd.to_datetime(db1['date']).dt.date
    db1['catch_volume'] = db1['catch_volume'] * 1000
    db1.drop(['date'], inplace=True, axis=1)
    #db1 = catch.merge(prod_type, on='id_fish').merge(product, on=['id_ves',	'date', 'id_prod_type'])
    return db1

def load_db2(path_to_file):
    ext1 = pd.read_csv(os.path.join(path_to_file, 'Ext.csv'))
    ext2 = pd.read_csv(os.path.join(path_to_file, 'Ext2.csv'))
    return ext1, ext2

def get_db2(ext1, ext2):
    #ext, ext2 = deduplication_db2(ext, ext2)
    ext2.loc[ext2.unit=='тонна','volume'] = ext2[ext2.unit=='тонна'] * 1000
    db2 = ext1.merge(ext2, on='id_vsd')
    db2['date'] = pd.to_datetime(db2.date_fishery).dt.date
    db2.drop(columns=['date_fishery'])
    return db2

def aggrid_interactive_table(df: pd.DataFrame):
    """Creates an st-aggrid interactive table based on a dataframe.

    Args:
        df (pd.DataFrame]): Source dataframe

    Returns:
        dict: The selected row
    """
    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enableSort=True
    )


    options.configure_side_bar()

    options.configure_selection("single")
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        theme="light",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )
    
    return selection


def filter_table(merged, selection):
    print(selection)
    return pd.DataFrame([['WAHOOOOO', 'Wahaa'], ['Wahaa', 'WAHOOOOO']], columns=['col1', 'col2'])


def calc_mismatch(data):
    data = data.copy()
    data['delta1'] = abs(data['catch_volume'] - data['volume_div_1000']) / data['catch_volume']
    data['delta2'] = abs(data['catch_volume'] - data['volume_div_100']) / data['catch_volume']
    data['delta3'] = abs(data['catch_volume'] - data['volume']) / data['catch_volume']

    data['mismatch, %'] = data.apply(lambda x: min(x['delta1'], x['delta2'], x['delta3']), axis=1)
    data['mismatch, %'] = (100 * data['mismatch, %']).round(2)
    return data.drop(columns=['delta1', 'delta2', 'delta3'])


def find_good_by_shift(data, db2_aggregated, shift=1):

    cols_to_drop = None
    if cols_to_drop is None:
        cols_to_drop = ['volume', 'volume_div_1000', 'volume_div_100', 'mismatch, %', 'is_abnormal']

    flag = data['is_abnormal'].astype(bool)

    orig_abnormal = data[flag].copy()
    orig_abnormal = orig_abnormal.drop(columns=cols_to_drop, errors='ignore')
    orig_abnormal = orig_abnormal.rename(columns={'date': 'as_is_date'})

    good_indexes = list()

    good_rows_list = []

    for i in range(1, shift+1):
        abnormal = orig_abnormal.reset_index()
        abnormal['date'] = pd.to_datetime(abnormal['as_is_date'])
        abnormal['date'] += pd.to_timedelta(f'{i}d')

        abnormal['date'] = abnormal['date'].dt.date

        data_shifted = db2_aggregated.merge(abnormal, on=['id_ves', 'id_fish', 'date'], how='inner')

        data_shifted = calc_mismatch(data_shifted)
        data_shifted['is_abnormal'] = data_shifted['mismatch, %'] > data_shifted['threshold_volume']

        good_rows = data_shifted[~data_shifted['is_abnormal']].copy()
        good_rows['is_abnormal'] = good_rows['is_abnormal'].astype(int)

        new_good_indexes = [ind for ind in good_rows['index'].to_list() if ind not in good_indexes]
        good_indexes += new_good_indexes
        good_rows_list.append(good_rows[good_rows['index'].isin(new_good_indexes)].copy())
    result = pd.concat(good_rows_list, ignore_index=True)
    result = result.drop(columns=['date'])
    return result.rename(columns={'as_is_date': 'date'})


def aggregate_db1_db2_table(db1, db2, threshold=25, shift=3):
    db1 = db1.copy()
    db2 = db2.copy()
    db1.columns = [col if col != 'catch_date' else 'date' for col in db1.columns]
    db1['date'] = pd.to_datetime(db1['date']).dt.date
    db1_aggregated = db1.groupby(['id_ves', 'date', 'id_fish'])['catch_volume'].sum().reset_index()

    db2_aggregated = db2.groupby(['id_ves', 'date', 'id_fish'])['volume'].sum().reset_index()

    db2_aggregated['volume_div_1000'] = db2_aggregated['volume'] / 1000
    db2_aggregated['volume_div_100'] = db2_aggregated['volume'] / 100

    joined_bases = db1_aggregated.merge(db2_aggregated, on=['id_ves', 'id_fish', 'date'], how='inner')
    fishes = db2[['id_fish', 'fish']].drop_duplicates()
    joined_bases = joined_bases.merge(fishes, on='id_fish', how='left')

    joined_bases = calc_mismatch(joined_bases)

    joined_bases['threshold_volume'] = threshold
    joined_bases['is_abnormal'] = joined_bases['mismatch, %'] > joined_bases['threshold_volume']
    joined_bases['is_abnormal'] = joined_bases['is_abnormal'].astype(int)

    new_good = find_good_by_shift(joined_bases, db2_aggregated, shift=shift)
    new_good_index = new_good['index'].to_list()
    new_good = new_good.drop(columns=['index'])
    joined_bases = joined_bases.drop(index=new_good_index)
    joined_bases = pd.concat([joined_bases, new_good]).sort_index()

    col_order = ['id_ves', 'date', 'id_fish', 'fish', 'catch_volume', 'volume',
              'volume_div_1000', 'volume_div_100', 'mismatch, %', 'threshold_volume', 'is_abnormal']

    joined_bases = joined_bases[col_order]

    col_names = ['id судна (id_ves)', 'дата', 'id рыбы','назавание рыбы', 'улов',
                'внесено в базу (размерность неизвестна)', 'внесено в базу (коррекция 1/1000)', 'внесено в базу (коррекция 1/100)',
                'отклонение внесенного от выловленного, %', 'порог отклонения, %', 'является ли подозрительным']

    colnames_map = dict(zip(col_order, col_names))

    joined_bases.columns = [colnames_map[col] for col in joined_bases.columns]

    joined_bases = joined_bases.loc[joined_bases['является ли подозрительным'] == 1, :].sort_values(by='отклонение внесенного от выловленного, %', ascending=False)

    return joined_bases
