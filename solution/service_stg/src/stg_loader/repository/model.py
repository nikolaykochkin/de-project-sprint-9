from datetime import datetime
from decimal import Decimal
from typing import List

from pydantic import BaseModel


class BaseModelId(BaseModel):
    id: str

    def __init__(self, **data):
        super().__init__(
            id=data.pop("_id", None) or data.pop("id", None),
            **data,
        )


class User(BaseModelId):
    name: str = None
    login: str = None


class Product(BaseModelId):
    name: str = None
    category: str = None
    price: Decimal = None
    quantity: int = None


class Restaurant(BaseModelId):
    name: str = None
    menu: List[Product] = None

    class Config:
        fields = {
            'menu': {'exclude': True},
        }


class Order(BaseModel):
    id: str
    date: datetime = None
    cost: Decimal = None
    payment: Decimal = None
    status: str = None
    restaurant: Restaurant = None
    user: User = None
    products: List[Product] = None


class OutputMessage(BaseModel):
    object_id: str
    object_type: str
    payload: Order
