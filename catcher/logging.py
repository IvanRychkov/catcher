import json
import pandas as pd


def json_write_results(d: dict, file: str):
    """TODO"""
    try:
        with open(file, 'r+') as f:
            origin = json.load(f)
            assert isinstance(origin, list), 'Original file is not iterable.'
            f.seek(0)
            json.dump(origin + [d], f)
    except FileNotFoundError:
        print('Creating new file', file)
        with open(file, 'w') as file:
            json.dump([d], file)


def json_load_results(file):
    """TODO"""
    with open(file) as file:
        return pd.DataFrame(json.load(file)).drop_duplicates()


__all__ = ['json_write_results', 'json_load_results']
