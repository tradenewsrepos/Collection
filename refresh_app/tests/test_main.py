from ..main import get_sleep_time
from datetime import datetime
def test_get_sleep_time():
    now = datetime.now()
    diff = datetime(now.year, now.month, now.day, 2, 0, 0, 0) - now
    assert diff == get_sleep_time()