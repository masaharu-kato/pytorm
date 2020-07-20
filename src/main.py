import dbtypes as dbt
import typing
import datetime



class Maker:
    name: str

class Category:
    name: str

class Shop:
    name: str
    url: str
    place: str

class Item:
    name: str
    maker: Maker
    category: Category

class Price:
    item: Item
    shop: Shop
    price: int
    shop_url: str

class Customer:
    name: str
    birthday: datetime.date
    address: str

class Review:
    item: Item
    customer: Customer






def main():
    pass


if __name__ == "__main__":
    main()
