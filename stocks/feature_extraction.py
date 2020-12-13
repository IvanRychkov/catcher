import pandas as pd
from tqdm import trange
from tqdm.notebook import tnrange


def profit(price_bought, current_price, broker_commission=0.03, as_bool=False):
    """Считает прибыль от продажи акции по текущей цене.
    as_bool возвращает наличие/отсутствие прибыли.
    Налоги не учитываются."""
    result = (current_price - price_bought - (current_price + price_bought) * broker_commission)
    return result > 0 if as_bool else result


def profit_chance(window, single_day=False, **profit_kws):
    """Возвращает вероятность прибыли в окне.
    single_day - ограничивает окно окончанием торгового дня."""
    assert isinstance(window.index, pd.DatetimeIndex), 'Not a time series.'
    # Сейчас 1 элемент
    now = window.index[0]
    # Если один день, то ограничиваем сегодняшним днём
    if single_day:
        future = window[window.index.dayofyear == now.dayofyear][1:]
    # Либо всё дальнейшее время
    else:
        future = window[1:]
    return (profit(window[now], future, as_bool=True, **profit_kws).sum() / future.shape[0]) if future.shape[
                                                                                                    0] > 0 else 0


def lookahead_agg(column, aggfunc, window_size=None, pbar='notebook', **agg_kws):
    """Выполняет агрегирование столбца с забеганием вперёд.
    pbar = {'notebook', None/False}."""

    # Выбираем полоску прогресса
    if pbar:
        if isinstance(pbar, str):
            if pbar.startswith('n'):
                range_func = tnrange
        else:
            range_func = trange
    else:
        range_func = range
    if window_size is None:
        window_size = column.shape[0]
    # Возвращаем агрегированные значения в сужающемся окне
    return pd.Series(
        data=(aggfunc(
            column[i:min(i + window_size, column.shape[0])], **agg_kws)
            for i in range_func(column.shape[0])),
        index=column.index)
