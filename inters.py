import typing


class IdbManager:
    @classmethod
    def sync_daily_exche(cls, currencies=None) -> tuple[typing.Any, int]:
        pass

    @classmethod
    def sync_exch_range(cls, start_date: str, end_date: str, CURRENCIES) -> tuple[typing.Any, int]:
        pass

    @classmethod
    def get_exch_range(cls, start_date: str, end_date: str, currencies) -> tuple[typing.Any, int]:
        pass