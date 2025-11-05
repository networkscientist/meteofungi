import re
from pathlib import Path
from re import Pattern

DATA_PATH: Path = Path(__file__).resolve().parents[2].joinpath('data')
TIMEZONE_SWITZERLAND_STRING: str = 'Europe/Zurich'
TIME_PERIOD_VALUES: tuple[int, ...] = (3, 7, 14, 30)
parameter_description_extraction_pattern: Pattern[str] = re.compile(r'([\w\s()]+)')
