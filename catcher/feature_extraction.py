import pandas as pd
import  numpy as np
from .timeseries import future_periods
from scipy.optimize import root


def lookahead_window(column, aggfunc, window_size=60, shift=0, **agg_kws):
    """Выполняет агрегирование столбца с забеганием вперёд."""
    # Переворачиваем выборку, агрегируем и разворачиваем обратно
    return column[::-1].shift(-shift).rolling(window_size, min_periods=1).agg(aggfunc, **agg_kws)[::-1]


def profit(buy_price: float,
           sell_price: float,
           broker_commission: float = 0.003,
           threshold=0,
           as_bool=False) -> [bool, float]:
    """Calculates profit from buying at buy_price and selling at sell_price.
    Doesn't count taxes as they are applied after profit.
    Args:
        buy_price (float): Price bought at.
        sell_price (float): Price to sell at.
        broker_commission (float): Service commission to be used in profit calculation.
        threshold (float): The smallest value to be considered as profit.
        as_bool (bool): To represent fact of profit >= threshold as bool.

    Returns:
        bool: if as_bool, returns the fact of profit with respect to threshold.
        float: the profit size.
    """
    assert threshold >= 0, 'Negative threshold will lead to money loss. Change it for at least zero value.'
    result = (sell_price - buy_price - (sell_price + buy_price) * broker_commission)
    return result >= threshold if as_bool else result


def min_price_for_profit(buy_price, broker_commission=0.003) -> float:
    """Calculates minimum price for getting profit from current buying price.

    Args:
        buy_price (float): the price we bought instrument for.
        broker_commission (float): service commission to be used in profit calculation.

    Returns:
        float
    """
    return np.round(root(lambda sell: profit(buy_price=buy_price,
                                             sell_price=sell,
                                             threshold=0,
                                             broker_commission=broker_commission),
                         x0=buy_price)['x'][0],
                    decimals=2)


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
    return lookahead_window(price_column, profit_chance_lookahead,
                            window_size=lookahead, broker_commission=0.0005).rename('buy')


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
