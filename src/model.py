import pandas as pd
from typing import  Optional

class model:
    def __init__(self) -> None:
        pass


    def get_susp_table(self) ->pd.DataFrame:
        """Метод, возвращаюший отсортированный спсиок полдозрительных кейсов

        Returns:
            pd.DataFrame: Датафрейм с колонками: ves_id, date, forecast, trashhold
        """        
        pass


def add_warning_type(tab: pd.DataFrame, trashhold: Optional[float]=None) -> pd.Series:
    """Возвращает тип опасности "Warning", 'Critical", "None"

    Args:
        tab (pd.DataFrame): _description_
        trashhold (Optional[float]): _description_

    Returns:
        pd.Series: _description_
    """