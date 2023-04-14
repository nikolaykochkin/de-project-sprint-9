from datetime import datetime
from decimal import Decimal
from typing import List

from pydantic import BaseModel


class User(BaseModel):
    id: str
    name: str = None
    login: str = None


class Product(BaseModel):
    id: str
    name: str = None
    category: str = None
    price: Decimal = None
    quantity: int = None


class Restaurant(BaseModel):
    id: str
    name: str = None


class Order(BaseModel):
    id: str
    date: datetime = None
    cost: Decimal = None
    payment: Decimal = None
    status: str = None
    restaurant: Restaurant = None
    user: User = None
    products: List[Product] = None


class InputMessage(BaseModel):
    object_id: str
    object_type: str
    payload: Order
