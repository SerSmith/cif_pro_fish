"""функции для исследовательского анализа данных
"""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def nan_estimation_graph(df,
                         autosize=True,
                         fig_width=8,
                         fig_height=8,
                         x_ticks_step=10, 
                         nan_values=None,
                         size_coef=0.3,
                         top=None):
    """Строит диаграмму пропущенных значений
    """
    df = df.copy()
    title_part = 'NaN'
    if nan_values is not None:
        replace_dict = {value: np.nan for value in nan_values}
        df = df.fillna('NaN')
        df = df.replace(replace_dict)
        title_part = f"{nan_values}"
    
    if df.isna().sum().sum() > 0:
        nan_df = pd.DataFrame()
        nan_df['percent'] = (100*df.isna().sum()/len(df)).sort_values()
        nan_df['count'] = df.isna().sum().sort_values()
        nan_df = nan_df[nan_df['count'] > 0]

        nan_percent_max = nan_df['percent'].max()

        if top is not None:
            nan_df = nan_df.tail(top)

        colors = (nan_df['percent']  / nan_percent_max ).tolist()
        #подготовка градиентной окраски
        colors = [(color, 0.5*(1-color), 0.5*(1-color)) for color in colors]

        if autosize:
            fig_height = int(nan_df.shape[0]*size_coef) + 2
        
        figsize=(fig_width, fig_height)

        plt.figure(figsize=figsize)
        plt.grid(alpha=0.8)
        plt.xticks(range(0, 100+x_ticks_step, x_ticks_step))
        plt.xlim(0, nan_percent_max + 5)
        plt.xlabel(f'% {title_part}')
        plt.ylim(-1, len(nan_df))
        plt.ylabel('признак')

        xpos = nan_df['percent'] + nan_percent_max*0.02

        bbox=dict(boxstyle="round", fc=(1, 1, 1, 0.8))

        for x, y, txt in zip( xpos, nan_df.index, nan_df['count'] ):
            plt.text(x, y, f'{txt} шт.', verticalalignment='center', bbox=bbox)

        plt.hlines(y=nan_df.index, xmin = 0, xmax = nan_df['percent'], alpha=0.7, 
                   linewidth=10, colors=colors)
        title = \
            f'Оценка количества и доли (%) {title_part} в данных\nВсего записей: {len(df)}, из них с {title_part}:'
        if top is not None:
            title = \
                f'TOP {top} количества и доли (%) {title_part} в данных\nВсего записей: {len(df)}, из них с {title_part}:'

        plt.title(title, 
                  size=14)
        plt.tight_layout()
        plt.show()
    else:
        print(f'В наборе данных нет {title_part} значений')