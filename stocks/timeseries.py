def working_hours(data, datetime_col=None, weekend_days=(5, 6)):
    """Оставляет только будние дни. По умолчанию использует индекс в качестве столбца с датой."""
    if not datetime_col:
        datetime_col = data.dropna().index
    return data.dropna()[(~datetime_col.dayofweek.isin(weekend_days)) &
                (datetime_col.hour.min() <= datetime_col.hour) &
                (datetime_col.hour <= datetime_col.hour.max())]


__all__ = ['working_hours']
