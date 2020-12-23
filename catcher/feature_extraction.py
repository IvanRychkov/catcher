import pandas as pd
from .timeseries import future_periods


def lookahead_window(column, aggfunc, window_size=60, shift=0, **agg_kws):
    """Выполняет агрегирование столбца с забеганием вперёд."""
    # Переворачиваем выборку, агрегируем и разворачиваем обратно
    return column[::-1].shift(-shift).rolling(window_size, min_periods=1).agg(aggfunc, **agg_kws)[::-1]


def profit(price_bought, current_price, broker_commission=0.003, as_bool=False):
    """Считает прибыль от продажи акции по текущей цене.
    as_bool возвращает наличие/отсутствие прибыли.
    Налоги не учитываются."""
    result = (current_price - price_bought - (current_price + price_bought) * broker_commission)
    return result > 0 if as_bool else result


def profit_chance_lookahead(window, **profit_kws):
    """Возвращает вероятность прибыли в окне. Совместима с lookahead_window.
    single_day - ограничивает окно окончанием торгового дня."""
    assert isinstance(window.index, pd.DatetimeIndex), 'Not a time series.'
    # Сейчас 1 элемент
    now = window.index[-1]

    # Будущее - все предыдущие строки
    future = window[:-1]
    return (profit(window[now], future, as_bool=True, **profit_kws).sum() / future.shape[0]) \
        if future.shape[0] > 0 else 0


def buy_recommendation(price_column, lookahead=120):
    """Генерирует рекомендацию к покупке."""
    return lookahead_window(price_column, profit_chance_lookahead, window_size=lookahead, broker_commission=0.0005)


def generate_features(data, price_column, future=True, rolling_periods=60):
    """Пайплайн для генерации признаков."""
    df = data[[price_column]]
    # Отсчёт
    if future:
        df['future'] = future_periods(data)
    # Среднее
    if rolling_periods > 0:
        df[f'rolling_avg_{rolling_periods}'] = data[price_column].rolling(rolling_periods, min_periods=1).mean()
    return df


__all__ = ['buy_recommendation', 'lookahead_window', 'profit', 'profit_chance_lookahead']
