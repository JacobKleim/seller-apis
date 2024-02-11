import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров магазина Ozon.

    Args:
        last_id (str): Идентификатор последнего товара, полученного в
        предыдущем запросе.
        client_id (str): Идентификатор клиента (магазина) на платформе Ozon.
        seller_token (str): Токен продавца для доступа к API Ozon.

    Returns:
        list: Список товаров магазина Ozon.

    Examples:
        >>> get_product_list("123", "your_client_id", "your_seller_token")
        [{'item_id': '456', 'name': 'Product 1', 'price': 100.0},
        ... {'item_id': '789', 'name': 'Product 2', 'price': 150.0}]
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает артикулы товаров магазина Ozon.

    Args:
        client_id (str): Идентификатор клиента (магазина) на платформе Ozon.
        seller_token (str): Токен продавца для доступа к API Ozon.

    Returns:
        list: Список артикулов товаров магазина Ozon.

    Examples:
        >>> get_offer_ids("your_client_id", "your_seller_token")
        ['123', '456', '789']
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров магазина Ozon.

    Args:
        prices (list): Список цен на товары для обновления.
        client_id (str): Идентификатор клиента (магазина) на платформе Ozon.
        seller_token (str): Токен продавца для доступа к API Ozon.

    Returns:
        dict: Результат обновления цен.

    Examples:
        >>> update_price([{'offer_id': '123', 'price': 120.0},
        ...     {'offer_id': '456', 'price': 160.0}],
        ...     "your_client_id", "your_seller_token")
        {'success': True, 'message': 'Prices updated successfully'}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет остатки товаров магазина Ozon.

    Args:
        stocks (list): Список уровней запасов товаров для обновления.
        client_id (str): Идентификатор клиента (магазина) на платформе Ozon.
        seller_token (str): Токен продавца для доступа к API Ozon.

    Returns:
        dict: Результат обновления остатков.

    Examples:
        >>> update_stocks([{'offer_id': '123', 'stock': 50},
        ...     {'offer_id': '456', 'stock': 30}],
        ...     "your_client_id", "your_seller_token")
        {'success': True, 'message': 'Stocks updated successfully'}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает файл 'ostatki' с сайта Casio.

    Извлекает данные и возвращает список остатков часов.

    Returns:
        list: Список остатков часов в виде словарей.

    Examples:
        >>> download_stock()
        [{'Код': '123', 'Наименование': 'Product 1',
        ... 'Цена': '5,990.00 руб.', 'Количество': '10'},
        ... {'Код': '456', 'Наименование': 'Product 2',
        ... 'Цена': '8,500.50 руб.', 'Количество': '1'}]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Создает список остатков товаров.

    Учитывает только те, которые загружены в магазин.

    Args:
        watch_remnants (list): Список остатков товаров извлеченных из файла.
        offer_ids (list): Список артикулов товаров магазина Ozon.

    Returns:
        list: Список уровней запасов товаров для обновления.

    Examples:
        >>> create_stocks(
        ...     [{'Код': '123', 'Наименование': 'Product 1',
        ...      'Цена': '5,990.00 руб.', 'Количество': '10'}],
        ...     ['123', '456']
        ... )
        [{'offer_id': '123', 'stock': 100}]
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен на товары.

    Учитывает только те, которые загружены в магазин.

    Args:
        watch_remnants (list): Список остатков товаров извлеченных из файла.
        offer_ids (list): Список артикулов товаров магазина Ozon.

    Returns:
        list: Список цен на товары для обновления.

    Examples:
        >>> create_prices(
        ...     [{'Код': '123', 'Наименование': 'Product 1',
        ...      'Цена': '5,990.00 руб.'}],
        ...     ['123', '456']
        ... )
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
        ... 'offer_id': '123', 'old_price': '0', 'price': '5990'}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразует строковое представление цены в удобный формат.

    Args:
        price (str): Строковое представление цены, например, "5'990.00 руб."

    Returns:
        str: Преобразованная цена в удобный формат, например, "5990"

    Raises:
        ValueError: Если `price` не содержит числовые значения.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'

        >>> price_conversion("10'500.50 руб.")
        '10500'

        >>> price_conversion("invalid_price")
        Traceback (most recent call last):
            ...
        ValueError: invalid literal for int() with base 10: 'invalid_price'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделить список lst на части по n элементов.

    Args:
        lst (list): Список для разделения.
        n (int): Размер каждой части.

    Yields:
        list: Части списка.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5, 6], 2))
        [[1, 2], [3, 4], [5, 6]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Обновляет цены на товары в магазине Ozon.

    Args:
        watch_remnants: Остатки товаров из файла.
        client_id: Идентификатор клиента.
        seller_token: Токен продавца.

    Returns:
        list: Список обновленных цен.

    Examples:
        >>> await upload_prices(watch_remnants, "your_client_id",
        ...     "your_seller_token")
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
        ... 'offer_id': '123', 'old_price': '0', 'price': '5990'}]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Обновляет остатки товаров в магазине Ozon.

    Args:
        watch_remnants: Остатки товаров из файла.
        client_id: Идентификатор клиента.
        seller_token: Токен продавца.

    Returns:
        tuple: Кортеж, содержащий два списка - обновленные остатки
        и непустые остатки.

    Examples:
        >>> await upload_stocks(
        ...     watch_remnants, "your_client_id", "your_seller_token"
        ... )
        (
            [{'offer_id': '123', 'stock': 100}],
            [{'offer_id': '123', 'stock': 100}]
        )
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
