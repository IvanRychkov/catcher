import pandas as pd
import datetime
import requests
from collections import namedtuple


def check_response(r):
    """Проверяет код ответа от API и возвращает JSON."""
    assert r.status_code == 200, r.json()['payload']['message']
    return r.json()


def preproc_pipeline(df):
    """Пайплайн предобработки цен на акции."""
    if df.shape[0] == 0:
        return df
    return df.assign(
        datetime=pd.to_datetime(df.time).dt.tz_localize(None) + pd.Timedelta('3h')
    ).drop(['interval', 'figi', 'time'],
           axis=1).set_index('datetime').rename(columns={'o': 'open',
                                                         'c': 'close',
                                                         'h': 'high',
                                                         'l': 'low',
                                                         'v': 'volume'})


def make_datetime(time: str = None):
    """Форматирует время для Тинькофф"""
    if not time:
        time = datetime.datetime.now()
    else:
        time = datetime.datetime.fromisoformat(time)
    return time


def strftime(time):
    return time.strftime('%Y-%m-%dT%H:%M:%S.%f+03:00')


class TinkoffAPI:
    """Класс для работы с API Тинькофф Инвестиций."""
    default_ticker = 'tcsg'
    Instrument = namedtuple('Instrument', 'figi ticker isin minPriceIncrement lot currency name type')

    # Таблица временных интервалов и их границ
    TIME_INTERVALS = pd.DataFrame(
        index=['1min', '2min', '3min', '5min', '10min', '15min', '30min', 'hour', 'day', 'week', 'month'],
        data={
            'max_length': map(pd.Timedelta, ['1d', '1d', '1d', '1d', '1d', '1d', '1d', '7d', '365d', '104w', '3650d']),
            'timedelta': map(pd.Timedelta,
                             ['1min', '2min', '3min', '5min', '10min', '15min', '30min', '1h', '1d', '1w', '30d'])}
    )

    def register_sandbox(self):
        """Регистрирует аккаунт в песочнице."""
        r = requests.post('https://api-invest.tinkoff.ru/openapi/sandbox/sandbox/register',
                          params={'brokerAccountType': 'Tinkoff'},
                          headers=self.header)
        self.account_id = check_response(r)['payload']['brokerAccountId']
        print('Sandbox account ID:', self.account_id)

    def remove_sandbox(self):
        """Удаляет аккаунт из песочницы."""
        r = requests.post('https://api-invest.tinkoff.ru/openapi/sandbox/sandbox/remove',
                          params={'brokerAccountId': self.account_id},
                          headers=self.header)
        check_response(r)
        print(f'Account "{self.account_id}" successfully removed.')

    @staticmethod
    def check_time_interval(interval):
        '''Checks whether time interval is valid.

        Args:
            interval (str): value that represents time interval.

        Returns:
            None

        Raises:
            AssertionError: when time interval is invalid.
        '''
        assert interval in TinkoffAPI.TIME_INTERVALS.index, f'Wrong time interval "{interval}". Accepted values are {TinkoffAPI.TIME_INTERVALS.index}.'

    def get_stock_prices(self, date=None, periods=None, batches: int = 1, ticker=None, interval='1min',
                         preprocess=preproc_pipeline):
        """Возвращает датафрейм с ценами в заданном интервале времени.

        Args:
            date (str, optional): end date to load stocks data up to.
            periods (int, optional): number of most recent periods available from selected date back.
            batches (int, optional): number of full consequent batches of data downloaded
            ticker (str, optional):  ticker to load data on.
            interval (str, optional): time window to aggregate stocks data. Available intervals are contained in TinkoffAPI.TIME_INTERVALS table.
            preprocess (func) - function that preprocesses data into desired form."""
        # Проверяем валидность интервала
        TinkoffAPI.check_time_interval(interval)

        data = []

        # Дельта во времени, если есть
        batch_length = (TinkoffAPI.TIME_INTERVALS.at[interval, 'timedelta'] * periods if periods
                        else TinkoffAPI.TIME_INTERVALS.at[interval, 'max_length'])

        # Батчи идут задом наперёд, чтобы скачивать данные в хронологическом порядке
        for batch_n in range(batches, 0, -1):
            # Инициализируем время в datetime
            if not date:
                dt = make_datetime()
            else:
                dt = make_datetime(date) + datetime.timedelta(days=1)

            # Время начала раньше времени конца на 1 батч
            start_date = strftime(dt - batch_length * (batch_n))

            # С каждым батчем время вычитается на 1 batch_length
            end_date = strftime(dt - batch_length * (batch_n - 1))

            #             print('from {} to {}'.format(start_date, end_date))

            # Делаем запрос
            r = requests.get('https://api-invest.tinkoff.ru/openapi/sandbox/market/candles',
                             params={'figi': self.get_figi_by_ticker(ticker=ticker) if ticker else self.instrument.figi,
                                     'from': start_date,
                                     'to': end_date,
                                     'interval': interval},
                             headers=self.header)
            # Пополняем список очередным батчем
            data.append(
                preprocess(pd.DataFrame(check_response(r)['payload']['candles']))
            )
        return pd.concat(data)

    def get_instrument_by_ticker(self, ticker=None):
        """Получает инструмент по тикеру.

        Args:
            ticker (str): Ticker to get information about.

        Returns:
            Instrument: namedtuple object containing info about market instrument.
        """
        r = requests.get('https://api-invest.tinkoff.ru/openapi/sandbox/market/search/by-ticker',
                         params={'ticker': ticker if ticker else self.instrument.ticker},
                         headers=self.header)
        return TinkoffAPI.Instrument(**check_response(r)['payload']['instruments'][0])

    def get_figi_by_ticker(self, ticker=None):
        """Получает FIGI инструмента."""
        return self.get_instrument_by_ticker(ticker=ticker).figi

    def __init__(self, ticker=default_ticker, token: str = None):
        """Создаёт контекст для работы с акцией."""
        if not token:
            print('Using sandbox token.')
            self.token = 't.kQ-0gYepAG1y4czOoZ02Bx_6Mj89IGkZfyFN5-sXYRwMLaKwIOhKMy8vysoOLf9OgDAvD0cgTFuZdNU63QLIbA'
        else:
            self.token = token

        self.header = {'Authorization': f'Bearer {self.token}'}
        self.account_id = None
        self.instrument = self.get_instrument_by_ticker(ticker)
        print('Selected instrument: {name}.'.format(name=self.instrument.name))

    def __str__(self):
        return f'Tinkoff API context for {self.instrument.ticker}'

    __repr__ = __str__


__all__ = ['check_response', 'preproc_pipeline', 'make_datetime', 'strftime', 'TinkoffAPI']