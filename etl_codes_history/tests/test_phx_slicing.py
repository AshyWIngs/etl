from datetime import datetime, timedelta
from scripts.db.phoenix_client import PhoenixClient  # используем реальный класс

class Dummy(PhoenixClient):
    """
    Лёгкий стаб для тестов нарезки: наследуем реальную реализацию, но
    не открываем сетевые соединения в __init__. Держим только настройку шага.
    """
    def __init__(self, initial_slice_sec: int):
        # Не зовём super().__init__ — тесты не должны трогать сеть.
        self._initial_slice_sec = int(initial_slice_sec)