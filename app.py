import datetime
from quart import Quart, jsonify, request
from core import PGdbManager
import croniter
import asyncio

# Cron-формат
# <Минуты> <Часы> <Дни_месяца> <Месяцы> <Дни_недели>
UPDATE_PERIOD = '* * * * *'

CURRENCIES = {'rub', 'eur', 'usd', 'jpy'}

async def sync_daily():
    while True:
        PGdbManager.sync_daily_exche()
        now = datetime.datetime.now()
        print(now, 'Daily data synchronized')

        iter = croniter.croniter(UPDATE_PERIOD, now)
        next_datetime = iter.get_next(datetime.datetime)
        remaining_seconds = (next_datetime - now).total_seconds()

        await asyncio.sleep(remaining_seconds)

app = Quart(__name__)

@app.route('/exchanges/sync/')
async def sync_exchange():
    startDate = request.args.get('startDate')
    endDate = request.args.get('endDate')
    answer, code = PGdbManager.sync_exch_range(startDate, endDate, CURRENCIES)
    return jsonify(answer), code

@app.route('/exchanges/')
async def get_exchange():
    startDate = request.args.get('startDate')
    endDate = request.args.get('endDate')
    currencies = request.args.get('currencies')
    if currencies:
        currencies = currencies.lower().split(',')
    else:
        currencies = CURRENCIES
    answer, code = PGdbManager.get_exch_range(startDate, endDate, currencies)
    return jsonify(answer), code

async def main():
    asyncio.create_task(sync_daily())
    await app.run_task()

if __name__ == '__main__':
    asyncio.run(main())