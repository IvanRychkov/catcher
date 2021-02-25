import pandas as pd
import numpy as np
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


def make_buy_features(data: pd.DataFrame,
                      price_column='open',
                      window_sizes=None,
                      shift_windows=True):
    """Produces window features for buy recommendation.
    Args:
        data (pd.DataFrame): table containing candle data.
        price_column (str): name of column in data representing candle opening price.
        window_sizes (iterable): list of window lenghts for rolling window calculations.
        shift_windows (bool): whether to shift rolling averages back in time. Causes data loss.

    Returns:
        pd.DataFrame: data updated with generated features.
    """
    features = data.copy()

    # Расширяющееся окно
    expanding_mean = features[price_column].expanding().mean()
    features['avg_deviation_exp'] = (features[price_column] - expanding_mean) / expanding_mean

    if window_sizes:
        try:
            iter(window_sizes)
        except TypeError:
            raise TypeError('window_sizes variable must be iterable.')
        for window in window_sizes:
            # Средние
            features[f'rolling_avg_{window}'] = (features[price_column]
                                                 .shift(-(window // 2) * shift_windows)
                                                 .rolling(window, win_type='bohman')
                                                 .mean())

            # Отклонение от среднего в процентах
            features[f'avg_deviation_{window}'] = (features[price_column] - features[f'rolling_avg_{window}']) / \
                                                  features[
                                                      f'rolling_avg_{window}']

            # Создаём окно
            rolling = features[price_column].rolling(window)

        # Отбрасываем скользящее среднее
        features.drop([col for col in features.columns if 'roll' in col and 'diff' not in col], axis=1, inplace=True)

    # Отбрасываем хвост с пропусками
    features.dropna(inplace=True)

    return features


def calc_cross_profit(data: pd.DataFrame, price_col='open', policy='lookahead', broker_commission=0.003,
                      profit_threshold=0):
    """Calculates profit cases for each combination of known buy and sell prices.
    Use after all the other features had been generated. Data must contain column with price values.

    Args:
        data (pd.DataFrame): Data to generate profit column.
        price_col (str, default 'open'): name of the column to get prices from.
        policy (str, optional, default 'lookahead'): approach to count possible profit cases.
            Accepted values are:
                {'lookahead', 'la'}: calculate profits only for future possible cases. Tends to be the most careful approach.
                {'lookbehind', 'lb'}: calculate profits as if we sold stocks in the past. Tends to be the most optimistic strategy.
                'full': compare each price in data to each price to calculate profit case. Fairly optimistic.
        broker_commission (float, optional): commission for buying and selling stocks. Included in calculating profits.
        min_lookahead (int): minimal number of observations to look ahead.
        profit_threshold (float, int): minimum profit to be considered as one.

    Returns:
        pd.DataFrame: original data cross-joined with itself and containing column that marks profit cases.
    """
    # Индекс обязательно должен быть datetime
    assert isinstance(data.index, pd.DatetimeIndex), 'Index must be of type pd.DateTimeIndex.'

    # И называться 'datetime'
    dt_str = 'datetime'

    # Автоматически переименовываем, если индекс не 'datetime'
    if data.index.name != dt_str:
        data.index.name = dt_str

    # Создадим вспомогательный датафрейм для внутренней работы
    df = data.reset_index()

    # Соединим каждую цену покупки с каждой ценой продажи
    data_cross = df.join(df[[price_col, dt_str]], how='cross', rsuffix='_sell')

    # Создадим признак-маркер будущего
    future = data_cross.datetime_sell > data_cross.datetime
    # Оставим только те строки, где цена продажи находится в будущем, если 'lookahead' или стратегия не задана
    if policy in ('lookahead', 'la') or not policy:
        data_cross = data_cross.loc[future]  # Дата покупки меньше даты продажи
    elif policy in ('lookbehind', 'lb'):
        data_cross = data_cross.loc[~future]  # Дата покупки меньше даты продажи
    elif policy in ('full', 'lookaround', 'lar'):
        data_cross['future'] = future.astype('int')

    # Посчитаем отсутствие убытков в каждом случае
    cross_profit = profit(buy_price=data_cross[price_col].values,
                          sell_price=data_cross[price_col + '_sell'].values,
                          broker_commission=broker_commission,
                          threshold=profit_threshold,
                          as_bool=True).astype('int8')

    return data_cross.set_index(  # Вернём на место индекс со временем
        'datetime'
    ).drop(  # Удалим столбцы с данными о продаже
        columns=[col for col in data_cross.columns if col.endswith('_sell')]
    ).assign(  # Добавим столбец с целевым признаком
        profit=cross_profit
    )


__all__ = ['buy_recommendation', 'lookahead_window',
           'make_buy_features', 'min_price_for_profit',
           'profit', 'profit_chance_lookahead']
