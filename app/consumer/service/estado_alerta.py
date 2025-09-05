import threading


class _EstadoAlerta:
    def __init__(self, initial=True):
        self._enabled = initial
        self._lock = threading.Lock()

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def set_enabled(self, value: bool) -> None:
        with self._lock:
            self._enabled = value


alert_state = _EstadoAlerta(initial=True)
