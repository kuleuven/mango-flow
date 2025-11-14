import logging
from dataclasses import asdict, dataclass

import humanize

logger = logging.Logger(__name__)


@dataclass
class TaskProgressPart:
    title: str = ""
    unit: str = ""
    start_time: float = 0.0
    current_time: float = 0.0
    total: int | float = 0
    value: int | float = 0
    name: str = ""

    # Not sure about the alternative counters, they could go into their own instance of the same class
    # alt_total: int | float | None = None
    # alt_total_unit: str | None = None
    # alt_total_pretty_func = str

    def __post_init__(self):
        if self.unit.lower() in ["byte", "bytes"]:
            self.pretty_func_size = humanize.naturalsize  # callable for pretty printing value
        else:
            self.pretty_func_size = humanize.metric
        self.pretty_func_time = (
            humanize.naturaltime
        )  # must accept also "future" True or False

    @property
    def elapsed(self) -> float:
        return self.current_time - self.start_time

    @property
    def speed(self, value=None) -> float:
        if value is None:
            value = self.value
        if self.elapsed:
            return self.value / self.elapsed
        else:
            return 0.0

    @property
    def eta(self):
        if self.speed:
            return (self.total - self.value) / self.speed
        return 0

    def set_pprint(self, pretty_func=None, pretty_func_time=None):
        if pretty_func:
            self.pretty_func_size = pretty_func
        if pretty_func_time:
            self.pretty_func_time = pretty_func_time

    def pprint_value(self, value=None) -> str:
        if value is None:
            value = self.value
        return self.pretty_func_size(value)

    def pprint_elapsed(self, value=None) -> str:
        if value is None:
            value = self.elapsed
        return self.pretty_func_time(value)

    def pprint_speed(self, value=None, end_with="/sec") -> str:
        if value is None:
            value = self.value
        if self.elapsed > 0:
            return self.pprint_value(value / self.elapsed) + end_with
        else:
            return "-/-"

    def pprint_eta(self, value=None) -> str:
        return self.pretty_func_time(
            self.eta,
            future=True,
        )

    def to_dict(self):
        return asdict(self)
