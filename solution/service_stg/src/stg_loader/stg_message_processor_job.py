import json
from datetime import datetime
from logging import Logger

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from lib.redis.redis_client import RedisClient
from stg_loader.repository.stg_repository import StgRepository


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
            self._stg_repository.order_events_insert(
                message.get('object_id'),
                message.get('object_type'),
                message.get('sent_dttm'),
                json.dumps(message.get('payload'))
            )
            output_message = self.get_output_message(message)
            self._producer.produce(output_message)

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def get_output_message(self, message: dict) -> dict:
        payload = message.get('payload')

        message_restaurant = payload.get('restaurant')
        restaurant = self._redis.get(message_restaurant.get('id'))
        message_restaurant['name'] = restaurant.get('name')

        message_user = payload.get('user')
        user = self._redis.get(message_user.get('id'))
        message_user['name'] = user.get('name')

        products = payload.get('order_items')
        menu = restaurant.get('menu')
        for product in products:
            for item in menu:
                if product.get('id') == item.get('_id'):
                    product['category'] = item.get('category')

        return {
            "object_id": message.get('object_id'),
            "object_type": message.get('object_type'),
            "payload": {
                "id": message.get('object_id'),
                "date": payload.get('date'),
                "cost": payload.get('cost'),
                "payment": payload.get('payment'),
                "status": payload.get('final_status'),
                "restaurant": message_restaurant,
                "user": message_user,
                "products": products
            }
        }
