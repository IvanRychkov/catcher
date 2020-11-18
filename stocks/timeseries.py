def working_days(data, datetime_col=None, weekend_days=(5, 6)):
    """Оставляет только будние дни. По умолчанию использует индекс в качестве столбца с датой."""
    if not datetime_col:
        datetime_col = data.index
    return data[~datetime_col.dayofweek.isin(weekend_days)]


__all__ = ['working_days']
