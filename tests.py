import pytest
from quart.testing import QuartClient
from app import app

@pytest.fixture
def client():
    return QuartClient(app)

@pytest.mark.asyncio
async def test_sync_exchange(client):
    response = await client.get('/exchanges/sync/')
    assert response.status_code == 200
    data = await response.get_json()

@pytest.mark.asyncio
async def test_sync_exchange_case_1(client):
    response = await client.get('/exchanges/sync/?startDate=2021-08-25&endDate=2022-12-31')
    assert response.status_code == 200
    data = await response.get_json()
    assert data == "Synchronization of 504 records completed successfully!"

@pytest.mark.asyncio
async def test_sync_exchange_case_2(client):
    response = await client.get('/exchanges/sync/?startDate=2020-08-25&endDate=2022-12-31')
    assert response.status_code == 200
    data = await response.get_json()
    assert data == "Synchronization of 755 records completed successfully!"

@pytest.mark.asyncio
async def test_sync_exchange_case_3(client):
    response = await client.get('/exchanges/sync/?startDate=2024-08-25&endDate=2021-12-31')
    assert response.status_code == 416
    data = await response.get_json()
    assert data == "Incorrect dates entered"

@pytest.mark.asyncio
async def test_sync_exchange_case_4(client):
    response = await client.get('/exchanges/sync/?endDate=2022-12-31')
    assert response.status_code == 416
    data = await response.get_json()
    assert data == "Incorrect dates entered"

@pytest.mark.asyncio
async def test_get_exchange(client):
    response = await client.get('/exchanges/')
    assert response.status_code == 200
    data = await response.get_json()

@pytest.mark.asyncio
async def test_get_exchange_data_case_1(client):
    response = await client.get('/exchanges/?startDate=2022-08-25&endDate=2023-12-31&currencies=rub,usd')
    assert response.status_code == 200
    data = await response.get_json()
    assert data == {"rub":{"avg":"0.022469","max":"0.023360","min":"0.021670"},
                    "usd":{"avg":"24.606855","max":"32.682000","min":"21.110000"}}

@pytest.mark.asyncio
async def test_get_exchange_data_case_2(client):
    response = await client.get('/exchanges/?startDate=2021-04-25&endDate=2022-09-19&currencies=rub,usd,jpy')
    assert response.status_code == 200
    data = await response.get_json()
    assert data == {"jpy":{"avg":"0.188207","max":"0.203980","min":"0.169880"},
                    "rub":{"avg":"0.186983","max":"0.318760","min":"0.022680"},
                    "usd":{"avg":"25.539154","max":"32.728000","min":"20.749000"}}


@pytest.mark.asyncio
async def test_get_exchange_data_case_3(client):
    response = await client.get('/exchanges/?startDate=2021-04-25&endDate=2022-09-31')
    assert response.status_code == 416
    data = await response.get_json()
    assert data == "Incorrect dates entered"


@pytest.mark.asyncio
async def test_get_exchange_data_case_4(client):
    response = await client.get('/exchanges/?endDate=2022-09-19')
    assert response.status_code == 416
    data = await response.get_json()
    assert data == "Incorrect dates entered"


@pytest.mark.asyncio
async def test_get_exchange_data_case_5(client):
    response = await client.get('/exchanges/?startDate=2023-04-25&endDate=2021-09-19')
    assert response.status_code == 416
    data = await response.get_json()
    assert data == "Incorrect dates entered"


if __name__ == '__main__':
    pytest.main()
