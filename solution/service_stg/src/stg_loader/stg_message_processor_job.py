import json
from datetime import datetime
from logging import Logger
from typing import Dict, Tuple, List

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from lib.redis.redis_client import RedisClient
from stg_loader.repository.model import User, Restaurant, OutputMessage, Order, Product
from stg_loader.repository.stg_repository import StgRepository


def get_products(payload: Dict, menu: List[Product]) -> List[Product]:
    products = []
    for item in payload.get('order_items'):
        product = Product.parse_obj(item)
        for i in menu:
            if product.id == i.id:
                product.category = i.category
                break
        products.append(product)
    return products


def get_order(message: Dict, restaurant: Restaurant, user: User) -> Order:
    payload = message.get('payload')
    products = get_products(payload, restaurant.menu)
    return Order(
        id=message.get('object_id'),
        date=payload.get('date'),
        cost=payload.get('cost'),
        payment=payload.get('payment'),
        status=payload.get('final_status'),
        restaurant=restaurant,
        user=user,
        products=products
    )


def get_output_message(message: Dict, restaurant: Restaurant, user: User) -> OutputMessage:
    return OutputMessage(
        object_id=message.get('object_id'),
        object_type=message.get('object_type'),
        payload=get_order(message, restaurant, user)
    )


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            message = self._consumer.consume()
            if not message:
                break
            if message.get("object_type") != "order":
                continue
            self.__save_message_to_stg(message)
            restaurant, user = self.__get_catalogs(message)
            output_message = get_output_message(message, restaurant, user)
            self._producer.produce(output_message.dict())

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def __save_message_to_stg(self, message: Dict) -> None:
        self._stg_repository.order_events_insert(
            message.get('object_id'),
            message.get('object_type'),
            message.get('sent_dttm'),
            json.dumps(message.get('payload'))
        )

    def __get_catalogs(self, message: Dict) -> Tuple[Restaurant, User]:
        restaurant_id = message.get('payload').get('restaurant').get('id')
        user_id = message.get('payload').get('user').get('id')
        restaurant_args, user_args = self._redis.mget(restaurant_id, user_id)
        return Restaurant.parse_obj(restaurant_args), User.parse_obj(user_args)
