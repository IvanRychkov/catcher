import pandas as pd
import datetime
import requests
from toads.utils import snake_case


class IEXAPI:
    """Класс для работы с API iexcloud."""
    valid_times = {'max', '5y', '2y', '1y', 'ytd', '6m', '3m', '1m', '1mm', '5d', '5dm', 'date', 'dynamic'}
    default_symbol = 'aapl'

    def __init__(self, symbol=default_symbol, token: str = None):
        """Создаёт контекст для работы с акцией."""
        if not token:
            # print('Using sandbox token.')
            self.token = 'Tpk_ef2b6bae433d4597a222690ed9fb967c'
        else:
            self.token = token

        self.symbol = symbol

    def __str__(self):
        return f'iexcloud API context for {self.symbol.upper()} stocks'

    __repr__ = __str__

    @staticmethod
    def format_time(string, **kws):
        """Принимает строку и превращает её в дату в необходимом формате."""
        return pd.to_datetime(string, **kws).date().isoformat().replace('-', '', -1)

    def get_chart(self, time_range=None, include_today=True, **kwargs):
        """Делает http-запрос к API iexcloud и возвращает его результат.

        time_range:
        max: All available data up to 15 years - Historically adjusted market-wide data
        5y: Five years - Historically adjusted market-wide data
        2y: Two years - Historically adjusted market-wide data
        1y: One year - Historically adjusted market-wide data
        ytd: Year-to-date - Historically adjusted market-wide data
        6m: Six months - Historically adjusted market-wide data
        3m: Three months - Historically adjusted market-wide data
        1m: One month (default) - Historically adjusted market-wide data
        1mm: One month - Historically adjusted market-wide data in 30 minute intervals
        5d: Five Days - Historically adjusted market-wide data by day.
        5dm: Five Days - Historically adjusted market-wide data in 10 minute intervals
        date: Specific date - If used with the query parameter chartByDay, then this returns historical OHLCV data for that date. Otherwise, it returns data by minute for a specified date, if available. Date format YYYYMMDD. Currently supporting trailing 30 calendar days of minute bar data.
        dynamic: One day - Will return 1d or 1m data depending on the day or week and time of day. Intraday per minute data is only returned during market hours.

        full doc at:
        https://iexcloud.io/docs/api/#historical-prices
        """
        # Адреса
        base_url = 'https://sandbox.iexapis.com/stable'
        endpoint = f'/stock/{self.symbol}/chart/'

        #  Разбираемся с параметрами
        params = {'token': self.token if self.token else 'Tpk_ef2b6bae433d4597a222690ed9fb967c',
                  'includeToday': 'true' if include_today else 'false'}

        if not time_range:
            time_range = 'dynamic'
        # Дата или диапазон
        if time_range not in IEXAPI.valid_times:
            params['exactDate'] = ''.join(IEXAPI.format_time(time_range))
        else:
            params['range'] = time_range

        # Если есть ключевые слова, дополним ими словарь
        params.update(kwargs)

        # Делаем запрос
        request = requests.get(base_url + endpoint, params=params)

        # Проверяем код запроса
        assert request.status_code == 200, f'Status code: {request.status_code}. Check your query parameters.'
        return request

    def get_chart_df(self, time_range='1m', **kwargs):
        """Делает http-запрос к iexcloud и формирует из него датафрейм.
        Подробная документация в методе Stocks.get_chart()."""
        # Делаем запрос
        return IEXAPI.make_df(self.get_chart(time_range=time_range, **kwargs).json())

    @staticmethod
    def make_df(json_request):
        """Формирует датафрейм из JSON."""

        def intraday():
            """Формирует датафрейм с поминутными данными."""
            data = pd.DataFrame(json_request)
            data.loc[:, 'datetime'] = pd.to_datetime(data.date + ' ' + data.minute)
            data = data.drop(['minute', 'date', 'label'], axis=1).set_index('datetime')
            return data

        def daily():
            """Формирует датафрейм с подневными данными."""
            data = pd.DataFrame(json_request)
            data.loc[:, 'datetime'] = pd.to_datetime(data.date)
            data = data.drop(['minute', 'date', 'label'], axis=1, errors='ignore').set_index('datetime')
            return data

        def dynamic():
            return pd.DataFrame(json_request['data'])

        # Для пустых запросов - пустой датафрейм
        if len(json_request) == 0:
            return pd.DataFrame()

        # Сначала пробуем поминутно
        try:
            if 'minute' in json_request[0].keys():
                return intraday(json_request)
        except:
            pass

        # Пробуем динамическое время
        try:
            if 'range' in json_request.keys():
                return dynamic()
        except:
            pass

        return daily(json_request)

    def get_n_last_dates(self, n=1, last_date=None, preproc_func=lambda x: x, eager=True):
        """Возвращает данные за последние n дней.

        eager - возвращает готовый датафрейм или генератор с ленивым вычислением для поочерёдного перебора.
        preproc_func - функция, предобрабатывающая каждый датафрейм. Должна возвращать датафрейм.
        """
        generator = (preproc_func(self.get_chart_df(IEXAPI.format_time(date)))
                     for date
                     in pd.date_range(end=last_date if last_date else datetime.date.today().isoformat(), periods=n))
        return pd.concat(list(generator)) if eager else generator


def preproc_pipeline(df: pd.DataFrame):
    """Функция предобработки для поминутных данных."""
    # Выбросим столбцы, в которых есть слово 'market'
    df.drop([col for col in df.columns if 'market' in col], axis=1, inplace=True)

    # Переименуем столбцы
    df.columns = df.columns.map(snake_case)

    return df.interpolate().fillna(0)


def load_data(ticker='aapl', preproc_func=lambda x: x):
    return IEXAPI(ticker).get_n_last_dates(3, preproc_func=preproc_func, eager=True)


__all__ = ['IEXAPI', 'preproc_pipeline', 'load_data']
