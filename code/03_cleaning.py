import pandas as pd
import pyparsing as pyp

from os import path

DATA_IN = "data/raw/semi-raw_data.csv"
DATA_OUT = "data/clean/signals.csv"

RENAMES = {
    'CROSSSTREE': 'cross_street',
    #'DESCRIPTIO': 'desription',
    'INTID': 'int_ID',
    'MAINSTREET': 'main_street',
    'MILEPOINT': 'milepoint',
    'OBJECTID': 'row_ID',
    'OWNER': 'owner',
    'ROUTE': 'route',
    'SIGID': 'sig_ID',
    'TIMES': 'times',
    'TYPE': 'type',
    'UNITID': 'unit_ID'
}

DROPS = ['OWNER2', 'DESCRIPTIO']

def read_in(path_to_data):
    assert path.exists(path_to_data)
    return pd.read_csv(path_to_data, index_col=0)

def clean_columns(df):
    df = df.drop(DROPS, axis=1)
    df = df.rename(RENAMES, axis=1)
    return df

def fix_times(df):
    df['TIMES'] = df['TIMES'].replace({"24 HOUR":"24 HOURS", "24HRS":"24 HOURS"})
    return df


route_prefix = pyp.Word(pyp.alphas)
route_suffix = pyp.Word(pyp.alphanums)
route_expr = route_prefix('route_prefix') + pyp.Optional('-') + route_suffix('route_suffix')

def normalize_route_value(route:str):
    parsed = route_expr.parse_string(route)
    return '-'.join((parsed.route_prefix, parsed.route_suffix))

def normalize_routes(df):
    df.ROUTE.update(df.ROUTE.dropna().apply(normalize_route_value))
    return df

def clean(df):
    df = fix_times(df)
    df = normalize_routes(df)
    df = clean_columns(df)
    return df

if __name__ == "__main__":
    df = read_in(DATA_IN)
    df = clean(df)
    df.to_csv(DATA_OUT)