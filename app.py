import datetime
from quart import Quart, jsonify, request
from core import PGdbManager, config
import croniter
import asyncio

db_manager = PGdbManager

async def sync_daily():
    while True:
        db_manager.sync_daily_exche()
        now = datetime.datetime.now()
        print(now, 'Daily data synchronized')

        cron_iter = croniter.croniter(config.UPDATE_PERIOD, now)
        next_datetime = cron_iter.get_next(datetime.datetime)
        remaining_seconds = (next_datetime - now).total_seconds()

        await asyncio.sleep(remaining_seconds)

app = Quart(__name__)


@app.route('/exchanges/sync/')
async def sync_exchange():
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    answer, code = db_manager.sync_exch_range(start_date, end_date, config.CURRENCIES)
    return jsonify(answer), code


@app.route('/exchanges/')
async def get_exchange():
    start_date = request.args.get('startDate')
    end_date = request.args.get('endDate')
    currencies = request.args.get('currencies')
    if currencies:
        currencies = currencies.lower().split(',')
    else:
        currencies = config.CURRENCIES
    answer, code = db_manager.get_exch_range(start_date, end_date, currencies)
    return jsonify(answer), code


async def main():
    asyncio.create_task(sync_daily())
    await app.run_task()

if __name__ == '__main__':
    asyncio.run(main())
