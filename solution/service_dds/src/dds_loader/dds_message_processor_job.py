from datetime import datetime
from logging import Logger

from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer

from dds_loader.repository.dds_repository import DdsRepository
from dds_loader.repository.model import InputMessage


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 final_order_status: str,
                 logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._final_order_status = final_order_status

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            message = self._consumer.consume()
            if not message:
                break

            input_message = InputMessage.parse_obj(message)
            self._dds_repository.save_message(input_message)
            if input_message.payload.status == self._final_order_status:
                stats = self._dds_repository.get_user_stats(input_message.payload.user)
                self._producer.produce(stats)

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
