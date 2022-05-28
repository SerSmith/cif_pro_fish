
import streamlit as st
import streamlit_funcs
import utils.helpers as helpers
import pandas as pd
from datetime import date

# st.session_state = {}

st.set_page_config(
    layout="centered", page_icon="🖱️", page_title="proto"
)

PATH_TO_DB1_FOLDER = './data/db1'
PATH_TO_DB2_FOLDER = './data/db2'

if 'has_shown' not in st.session_state:
    st.session_state['has_shown'] = True
    st.info(
    f"""
        Сейчас происходит первичная инициализация данных, это может занять до 5 минут, не пугайтесь :), во время работы задержки будут сильно меньше.
        """
    )

if 'db1' not in st.session_state:
    st.session_state['db1'] = streamlit_funcs.load_db1(PATH_TO_DB1_FOLDER)

if 'db2' not in st.session_state:
    ext1, ext2 = streamlit_funcs.load_db2(PATH_TO_DB2_FOLDER)

    # ext1, ext2 = helpers.deduplication_db2(ext1, ext2)
    # st.session_state['db2'] = streamlit_funcs.get_db2(ext1, ext2)

    st.session_state['db2'] = pd.read_pickle("db2.pkl")


mode = st.sidebar.selectbox("Режим поиска", options=['Несоответствия с известным id', 'Несоответствия по весу'])

st.sidebar.write("Загрузка данных")

ext1_flow = st.sidebar.file_uploader('ext1')
ext2_flow = st.sidebar.file_uploader('ext2')

update_data = st.sidebar.button('Обновить', key='update_data')




if mode == 'Несоответствия с известным id':
    diff = st.sidebar.slider('Введите допустимое отклонение', min_value=0., max_value=1., step=0.01, value=0.01)

    if update_data:
        ext1 = pd.read_csv(ext1_flow)
        ext2 = pd.read_csv(ext2_flow)
        ext1, ext2 = helpers.deduplication_db2(ext1, ext2)
        st.session_state['db2'] = streamlit_funcs.get_db2(ext1, ext2)
        st.session_state['filtered_db1'], st.session_state['merged'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)
        st.session_state['anomalies_with_keys'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)

    update_input = st.sidebar.button('Обновить', key='update_input')
    if update_input:
        st.session_state['anomalies_with_keys'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)
    
    st.header('Расхождения между базами по данным с одинаковыми ключами')
    selection = streamlit_funcs.aggrid_interactive_table(st.session_state['anomalies_with_keys'])

else:

    st.sidebar.write("Параметры поиска")
    date = st.sidebar.date_input('Введите дату', value=date(2022, 1, 1))
    diff = st.sidebar.slider('Введите допустимое отклонение', min_value=0., max_value=1., step=0.01, value=0.01)

    if update_data:
        ext1 = pd.read_csv(ext1_flow)
        ext2 = pd.read_csv(ext2_flow)
        ext1, ext2 = helpers.deduplication_db2(ext1, ext2)
        st.session_state['db2'] = streamlit_funcs.get_db2(ext1, ext2)
        st.session_state['filtered_db1'], st.session_state['merged'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)
        st.session_state['anomalies_with_keys'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)

    update_input = st.sidebar.button('Обновить', key='update_input')
    if update_input:
        st.session_state['filtered_db1'], st.session_state['merged'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)


    if 'filtered_db1' not in st.session_state:
        st.session_state['filtered_db1'], st.session_state['merged'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)

    # st.title("Проторип команды Optimists")

    st.header('Данные 1 базы, которые не находятся во 2 базе')
    selection = streamlit_funcs.aggrid_interactive_table(df=st.session_state['filtered_db1'])

    st.header('Наиболее похожие строчки на выбранную')

    chosen_closest = helpers.find_close(st.session_state['merged'], selection)

    streamlit_funcs.aggrid_interactive_table(df=chosen_closest)
