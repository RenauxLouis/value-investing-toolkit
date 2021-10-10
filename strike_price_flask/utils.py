from datetime import datetime


def get_current_date_time_as_prefix():
    now = datetime.utcnow()
    time_now = str(now)[:-7].replace(" ", "_").replace(
        ":", ""
    ).replace("-", "") + "_"

    return time_now
