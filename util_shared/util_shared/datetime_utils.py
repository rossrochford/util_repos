import datetime

import pytz


def get_utc_now():
    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
