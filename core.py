import datetime
import re
import requests
import pandas as pd
from sqlalchemy import create_engine, select, between, func
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text
import io

import config


class DataProvider:
    BASE_URL = 'https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing'

    @classmethod
    def get_daily_exchange(cls, date: str = None):
        param = ''
        if date:
            param = f'?date={date}'

        response = requests.get(
            f'{cls.BASE_URL}/daily.txt{param}')
        if response.status_code == 200:
            return pd.read_csv(io.BytesIO(response.content[response.content.index(b'\n')+1:]), sep='|')
        else:
            raise requests.RequestException(f'Нет данных для указанной даты {date}')

    @classmethod
    def get_year_exchange(cls, year: str | int = None):
        param = ''
        if year:
            param = f'?year={year}'

        response = requests.get(
            f'{cls.BASE_URL}/year.txt{param}')
        if response.status_code == 200:
            return pd.read_csv(io.BytesIO(response.content), sep='|')
        else:
            raise requests.RequestException(f'Нет данных для указанного года {year}')

    @classmethod
    def get_range_exchange(cls, start_date: datetime.date, end_date: datetime.date, CURRENCIES):
        df_parts = []
        for year in range(start_date.year, end_date.year + 1):
            df = cls.get_year_exchange(year)
            for column in df.columns:
                if column != 'Date':
                    if (num := int(column.split()[0])) > 1:
                        df[column] = df[column].map(
                            lambda exch: float(exch)/num if re.match("^\d+?\.\d+?$", str(exch)) else exch)
                else:
                    df[column] = df[column].map(
                        lambda date: datetime.date.fromisoformat(
                            '-'.join(reversed(date.split('.')))) if date != 'Date'
                        else date)
            df = df.rename(mapper=lambda col: col.split()[-1].lower(), axis='columns')
            df_parts.append(df[['date'] + list(CURRENCIES & set(df.columns))])
        result = pd.concat(df_parts, axis=0)
        return result[result['date'] != 'Date']


class PGdbManager:
    engine = create_engine(config.dbURL)
    table: Table = Table(config.TABLE, MetaData(), autoload_with=engine)

    @classmethod
    def add_columns(cls, columns):
        currencies = list(set(columns) - set(map(lambda col: str(col).split('.')[-1], [*cls.table.c])))
        if len(currencies) == 0:
            return
        stmt = text(f'''
        ALTER TABLE {config.TABLE}
        {', '.join([f'ADD COLUMN IF NOT EXISTS {cur} numeric(10, 6) NULL' for cur in currencies])}
        ;''')
        print(f'Columns added to the database: {currencies}')
        with cls.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    @classmethod
    def sync_daily_exche(cls, currencies=None):
        try:
            exchanges = DataProvider.get_daily_exchange()
            data = exchanges.to_dict(orient='records')
            if not currencies:
                insert_vals = {dct['Code'].lower(): dct['Rate']/dct['Amount'] for dct in data}
            else:
                insert_vals = {dct['Code'].lower(): dct['Rate']/dct['Amount'] for dct in data
                               if dct['Code'].lower() in currencies}
            cls.add_columns(list(insert_vals.keys()))
            insert_vals['date'] = datetime.date.today()
            stmt = insert(cls.table).values(insert_vals).on_conflict_do_nothing()
            with cls.engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
            return f'Synchronization of {len(exchanges)} records completed successfully!', 200
        except:
            return 'Synchronization error', 500

    @classmethod
    def sync_exch_range(cls, start_date: str, end_date: str, CURRENCIES):
        try:
            start_date, end_date = date_validate_or_today(start_date, end_date)
        except:
            return 'Incorrect dates entered', 416
        try:
            exchanges = DataProvider.get_range_exchange(start_date, end_date, CURRENCIES)
            cls.add_columns(exchanges.columns)
            insert_vals = exchanges.to_dict(orient='records')
            stmt = insert(cls.table).values(insert_vals).on_conflict_do_nothing()
            # stmt = stmt.on_conflict_do_update(
            #         constraint=cls.table.c.date,
            #         set_=stmt.excluded
            #     )
            with cls.engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
            return f'Synchronization of {len(exchanges)} records completed successfully!', 200
        except:
            return 'Synchronization error', 500

    @classmethod
    def get_exch_range(cls, start_date: str, end_date: str, currencies):
        try:
            start_date, end_date = date_validate_or_today(start_date, end_date)
        except:
            return 'Incorrect dates entered', 416
        currencies = list(set(currencies) & set(map(lambda col: str(col).split('.')[-1], [*cls.table.c])))
        if len(currencies) == 0:
            return 'Currencies not found', 404
        select_cols = []
        for col in currencies:
            select_cols.extend([func.min(cls.table.c[col]).label(col+'_min'),
                                (func.sum(cls.table.c[col])/func.count(cls.table.c[col])).label(col+'_avg'),
                                func.max(cls.table.c[col]).label(col+'_max')])
        s = select(
            *select_cols)\
            .where(between(cls.table.c.date, start_date, end_date))
        with cls.engine.connect() as conn:
            exchanges = conn.execute(s)
        result = {}
        row = exchanges.fetchone()
        for i, cur in zip(range(0, len(currencies)*3, 3), currencies):
            result[cur] = {'min': row[i], 'avg': round(row[i+1], 6) if row[i+1] is not None else None, 'max': row[i+2]}
        return result, 200


def date_validate_or_today(start_date: str, end_date: str):
    try:
        if not start_date:
            start_date = datetime.date.today()
        else:
            start_date = datetime.date.fromisoformat(start_date)
        if not end_date:
            end_date = datetime.date.today()
        else:
            end_date = datetime.date.fromisoformat(end_date)
        if start_date > end_date:
            raise
    except:
        raise
    return start_date, end_date
