import pandas as pd
import datetime


def last_day(data):
    """Выбирает последний день из данных."""
    last_date = data.index[-1]
    return data.loc[(data.index.day == last_date.day) & (data.index.year == last_date.year)].copy()


def working_hours(data, datetime_col=None, weekend_days=(5, 6)):
    """Оставляет только будние дни. По умолчанию использует индекс в качестве столбца с датой."""
    if not datetime_col:
        datetime_col = data.dropna().index
    return data.dropna()[(~datetime_col.dayofweek.isin(weekend_days)) &
                (datetime_col.hour.min() <= datetime_col.hour) &
                (datetime_col.hour <= datetime_col.hour.max())]


def split_day(data, split_hour=13):
    """Возвращает кортеж из данных, разделённых по часу."""
    return data.loc[data.index.hour < split_hour], data.loc[data.index.hour >= split_hour]


def future_periods(data):
    """Обратный отсчёт. Показывает, сколько осталось времени до конца торгов."""
    return pd.Series(data=range(data.shape[0] - 1, -1, -1), index=data.index)


def datetime_append(date=None, hours=15, minutes=59):
    """Возвращает заданное время дня. По умолчанию берёт сегодня."""
    day = datetime.date.today() if date is None else datetime.date.fromisoformat(date)
    return pd.to_datetime(datetime.datetime(year=day.year,
                                            month=day.month,
                                            day=day.day,
                                            hour=hours,
                                            minute=minutes))


def minutes_diff(start: pd.Timestamp, end: pd.Timestamp):
    """Возвращает количество минут между датами."""
    return (end - start).seconds // 60


__all__ = ['datetime_append', 'last_day', 'future_periods', 'minutes_diff', 'split_day', 'working_hours']
