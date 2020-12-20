import pandas as pd
import datetime
import requests


def check_response(r):
    """Проверяет код ответа от API и возвращает JSON."""
    assert r.status_code == 200, r.json()['payload']['message']
    return r.json()


def preproc_pipeline(df):
    """Пайплайн предобработки цен на акции."""
    if df.shape[0] == 0:
        return df
    return df.assign(
        datetime=pd.to_datetime(df.time).dt.tz_localize(None) + pd.Timedelta(hours=3)
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
    default_ticker = 'aapl'
    time_intervals = '1min', '2min', '3min', '5min', '10min', '15min', '30min', 'hour', 'day', 'week', 'month'

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

    def get_stock_prices(self, date=None, ticker=None, interval='1min', preprocess=preproc_pipeline):
        """Возвращает датафрейм с ценами в заданном интервале времени.
        preprocess - принимает и возвращает датафрейм."""
        assert interval in TinkoffAPI.time_intervals, 'Wrong time interval.'

        # Форматируем время
        date = make_datetime(date)
        start_date = strftime(date)
        end_date = strftime(date + datetime.timedelta(days=1))

        # Делаем запрос
        r = requests.get('https://api-invest.tinkoff.ru/openapi/sandbox/market/candles',
                         params={'figi': self.get_figi_by_ticker(ticker=ticker),
                                 'from': start_date,
                                 'to': end_date,
                                 'interval': interval},
                         headers=self.header)
        return preprocess(pd.DataFrame(check_response(r)['payload']['candles']))

    def get_figi_by_ticker(self, ticker=None):
        """Получает FIGI инструмента."""
        r = requests.get('https://api-invest.tinkoff.ru/openapi/sandbox/market/search/by-ticker',
                         params={'ticker': ticker if ticker else self.ticker},
                         headers=self.header)
        return check_response(r)['payload']['instruments'][0]['figi']

    def __init__(self, ticker=default_ticker, token: str = None):
        """Создаёт контекст для работы с акцией."""
        if not token:
            print('Using sandbox token.')
            self.token = 't.kQ-0gYepAG1y4czOoZ02Bx_6Mj89IGkZfyFN5-sXYRwMLaKwIOhKMy8vysoOLf9OgDAvD0cgTFuZdNU63QLIbA'
        else:
            self.token = token

        self.ticker = ticker
        self.header = {'Authorization': f'Bearer {self.token}'}
        self.account_id = None

    def __str__(self):
        return f'Tinkoff API context for {self.ticker.upper()} stocks'

    __repr__ = __str__


__all__ = ['check_response', 'preproc_pipeline', 'make_datetime', 'strftime', 'TinkoffAPI']