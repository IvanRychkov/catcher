import pandas as pd


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


def remaining_periods(data):
    """Обратный отсчёт. Показывает, сколько осталось времени до конца торгов."""
    return pd.Series(data=range(data.shape[0], 0, -1), index=data.index) + 1


__all__ = ['last_day', 'remaining_periods', 'split_day', 'working_hours']
