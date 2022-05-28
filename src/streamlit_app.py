
import streamlit as st
import streamlit_funcs
import utils.helpers as helpers
import streamlit_funcs
import pandas as pd
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

    ext1, ext2 = helpers.deduplication_db2(ext1.head(100), ext2.head(100))
    st.session_state['db2'] = streamlit_funcs.get_db2(ext1, ext2)

st.sidebar.write("Параметры поиска")
date = st.sidebar.date_input('Введите дату')
diff = st.sidebar.slider('Введите допустимое отклонение', min_value=0., max_value=1., step=0.01)

st.sidebar.write("Загрузка данных")

ext1_flow = st.sidebar.file_uploader('ext1')
ext2_flow = st.sidebar.file_uploader('ext2')

update = st.sidebar.button('Обновить')

if update:
    ext1 = pd.read_csv(ext1_flow)
    ext2 = pd.read_csv(ext2_flow)
    st.session_state['db2'] = streamlit_funcs.get_db2(ext1, ext2)
    st.session_state['filtered_db1'], st.session_state['merged'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)

if 'filtered_db1' not in st.session_state:
    st.session_state['filtered_db1'], st.session_state['merged'] = helpers.match(st.session_state['db1'], st.session_state['db2'], date, diff, window=3)

# st.title("Проторип команды Optimists")

st.header('Данные 1 базы, которые ме находятся в 2 базе')
selection = streamlit_funcs.aggrid_interactive_table(df=st.session_state['filtered_db1'])

st.header('Наиболее похожие строчки на выбранную')
chosen_closest = streamlit_funcs.filter_table(st.session_state['merged'], selection)

streamlit_funcs.aggrid_interactive_table(df=chosen_closest)
