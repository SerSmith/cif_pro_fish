from abc import ABC

class StObj(ABC):
    """Абстрактнуй класс, для отрисовки в стримлите
    """
    def __init__(self) -> None:
        pass

    @abstractmethod
    def initialize(self, **kwargs) -> None:
        """Метод, подгружающий данные - запускается один раз при стартее интерфейса
        """    
        pass

    @abstractmethod
    def get_st_draw(self, st) -> None:
        """Метод, отрисовывающий свой блок информации

        Args:
            st (_type_): streamlit context
        """        
        pass

class CaseAnalysys(StObj):
    """Визуализатор кейсв для анализа
    """    
        
    def __init__(self) -> None:
        pass

    def set_case(self, **kwargs) -> None:
        """данные о кейсе
        """        
        pass

class BuisnessRule(StObj):
    """Визуализатор бизнесс правил
    """

    def __init__(self) -> None:
        pass
    

