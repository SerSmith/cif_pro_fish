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


def aggregate_db1_db2_table(db1, ext1, ext2, threshold=25):
    db1.columns = [col if col != 'catch_date' else 'date' for col in db1.columns]
    db1['date'] = pd.to_datetime(db1['date']).dt.date
    db1_aggregated = db1.groupby(['id_ves', 'date', 'id_fish'])['catch_volume'].sum().reset_index()

    ext1.columns = [col if col != 'date_fishery' else 'date' for col in ext1.columns]
    ext1['date'] = pd.to_datetime(ext1['date']).dt.date
    db2_merged = ext2.merge(ext1, left_on=['id_vsd'], right_on=['id_vsd'], suffixes=['_ext', '_ext2'], how='left')

    db2_aggregated = db2_merged.groupby(['id_ves', 'date', 'id_fish'])['volume'].sum().reset_index()

    joined_bases = db1_aggregated.merge(db2_aggregated, on=['id_ves', 'id_fish', 'date'], how='inner')

    joined_bases['volume_div_1000'] = joined_bases['volume'] / 1000
    joined_bases['volume_div_100'] = joined_bases['volume'] / 100
    joined_bases['delta1'] = abs(joined_bases['catch_volume'] - joined_bases['volume_div_1000']) / joined_bases['catch_volume']
    joined_bases['delta2'] = abs(joined_bases['catch_volume'] - joined_bases['volume_div_100']) / joined_bases['catch_volume']
    joined_bases['delta3'] = abs(joined_bases['catch_volume'] - joined_bases['volume']) / joined_bases['catch_volume']

    joined_bases['mismatch, %'] = joined_bases.apply(lambda x: min(x['delta1'], x['delta2'], x['delta3']), axis=1)
    joined_bases['mismatch, %'] = (100 * joined_bases['mismatch, %']).round(2)

    joined_bases = joined_bases.drop(columns=['delta1', 'delta2', 'delta3'])

    joined_bases['threshold_volume'] = threshold
    joined_bases['is_abnormal'] = joined_bases['mismatch, %'] > joined_bases['threshold_volume']
    joined_bases['is_abnormal'] = joined_bases['is_abnormal'].astype(int)

    return joined_bases
