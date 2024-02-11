import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список продуктов из кампании на Yandex.Market.

    Args:
        page (str): Токен страницы для получения следующей порции продуктов.
        campaign_id (str): Идентификатор кампании на Yandex.Market.
        access_token (str): Токен доступа для аутентификации в API.

    Returns:
        dict: Словарь с результатами API-запроса, содержащий список продуктов.

    Example:
        >>> get_product_list("123", "your_campaign_id", "your_access_token")
        [{'item_id': '456', 'name': 'Product 1', 'price': 100.0},
        ... {'item_id': '789', 'name': 'Product 2', 'price': 150.0}]
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет данные о запасах товаров на Yandex.Market.

    Args:
        stocks (list): Список данных о запасах товаров для обновления.
        campaign_id (str): Идентификатор кампании на Yandex.Market.
        access_token (str): Токен доступа для аутентификации в API.

    Returns:
        dict: Словарь с результатами API-запроса.

    Example:
        >>> update_stocks([{"sku": "123", "warehouseId": "456",
        "items": [
            {"count": 10, "type": "FIT", "updatedAt": "2022-01-01T00:00:00Z"}]}
            ],
        "your_campaign_id", "your_access_token")
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены на товары на Yandex.Market.

    Args:
        prices (list): Список данных о ценах товаров для обновления.
        campaign_id (str): Идентификатор кампании на Yandex.Market.
        access_token (str): Токен доступа для аутентификации в API.

    Returns:
        dict: Словарь с результатами API-запроса.

    Example:
        >>> update_price([{"id": "123",
        ...     "price": {"value": 100, "currencyId": "RUR"}}],
        ...     "your_campaign_id", "your_access_token")
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает артикулы товаров из кампании на Yandex.Market.

    Args:
        campaign_id (str): Идентификатор кампании на Yandex.Market.
        market_token (str): Токен доступа к API Yandex.Market.

    Returns:
        list: Список артикулов товаров.

    Example:
        >>> get_offer_ids("your_campaign_id", "your_market_token")
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создает данные о запасах товаров для обновления на Yandex.Market.

    Args:
        watch_remnants (list): Список данных об остатках товаров.
        offer_ids (list): Список артикулов товаров для обновления.
        warehouse_id (str): Идентификатор склада.

    Returns:
        list: Список данных о запасах товаров.

    Example:
        >>> create_stocks([{"Код": "123", "Количество": 5}], ["123"],
        ...     "your_warehouse_id")
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает данные о ценах товаров для обновления на Yandex.Market.

    Args:
        watch_remnants (list): Список данных об остатках товаров.
        offer_ids (list): Список артикулов товаров для обновления.

    Returns:
        list: Список данных о ценах товаров.

    Example:
        >>> create_prices([{"Код": "123", "Цена": "50.00"}], ["123"])
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Асинхронно загружает цены на товары на Yandex.Market.

    Args:
        watch_remnants (list): Список данных об остатках товаров.
        campaign_id (str): Идентификатор кампании на Yandex.Market.
        market_token (str): Токен доступа к API Yandex.Market.

    Returns:
        list: Список данных о загруженных ценах.

    Example:
        >>> await upload_prices([{"Код": "123", "Цена": "50.00"}],
        ...     "your_campaign_id", "your_market_token")
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token,
                        warehouse_id):
    """
    Асинхронно загружает данные о запасах товаров на Yandex.Market.

    Args:
        watch_remnants (list): Список данных об остатках товаров.
        campaign_id (str): Идентификатор кампании на Yandex.Market.
        market_token (str): Токен доступа к API Yandex.Market.
        warehouse_id (str): Идентификатор склада.

    Returns:
        tuple: Кортеж, содержащий список не пустых запасов
        и общий список запасов.

    Example:
        >>> await upload_stocks([{"Код": "123", "Количество": 5}],
        ...     "your_campaign_id", "your_market_token", "your_warehouse_id")
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
