import time


class RunLog:
    def __init__(self):
        self._steps = []
        self._current = None
        self._t0 = time.perf_counter()

    def step(self, name):
        self._end_current()
        self._current = name
        self._step_start = time.perf_counter()
        print(f"→ {name}...")

    def _end_current(self):
        if self._current is not None:
            elapsed = time.perf_counter() - self._step_start
            self._steps.append((self._current, elapsed))
            print(f"  done {self._format(elapsed)}")

    def finish(self):
        self._end_current()
        total = time.perf_counter() - self._t0
        print("\n--- timing ---")
        for name, elapsed in self._steps:
            print(f"  {name}: {self._format(elapsed)}")
        print(f"  total: {self._format(total)}")
        return total

    @staticmethod
    def _format(seconds):
        if seconds < 60:
            return f"{seconds:.1f}s"
        return f"{seconds / 60:.1f}m"
