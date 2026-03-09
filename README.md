# Лабораторная работа: Потоковая обработка данных с Apache Kafka

## Цель работы
Научиться создавать приложения, использующие средства потоковой обработки данных в реальном времени.

## Задачи
- Развернуть приложение Apache Kafka.
- Разработать скрипт поставщика данных (Kafka Producer).
- Разработать скрипт потребителя данных (Kafka Consumer).
- Реализовать полный цикл работы с данными.

## Выполнение работы

### 1. Установка и запуск Apache Kafka
1. Скачана актуальная версия Apache Kafka с официального сайта.
2. Запущен Zookeeper (команда `bin/zookeeper-server-start.sh config/zookeeper.properties`).
3. Запущен сервер Kafka (`bin/kafka-server-start.sh config/server.properties`).

На скриншоте ниже показан запущенный сервер Kafka (консоль с логами):

<img width="797" height="334" alt="image" src="https://github.com/user-attachments/assets/c1efc75f-ac90-4c89-8ba0-d380baa24923" />

### 2. Разработка Producer'а (поставщика данных)
Скрипт `producer.py` выполняет следующие функции:
- Генерирует тестовое сообщение с информацией о результате тестирования студента (в соответствии с вариантом задания – тема «Тестирование»).  
  Сообщение содержит данные студента, данные теста, полученный балл, максимальный балл, процент выполнения и признак сдачи.
- Функция `generate_test_result()` формирует словарь с данными.
- Сериализация в JSON выполняется функцией `serialize_data()`.
- Отправка сообщения в топик `testing-topic` производится с помощью библиотеки `confluent_kafka`.
- После успешной отправки сообщение дублируется в консоль.

**Листинг `producer.py`** (основные части):
```python
import json
import time
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'testing-producer'
}
producer = Producer(config)

def generate_test_result():
    # ... формирование данных ...
    return {
        'result_id': 111222333,
        'timestamp': int(time.time()),
        'student': {...},
        'test': {...},
        'score': 85,
        'max_score': 100,
        'percentage': 85,
        'is_passed': True
    }

def send_message(topic, data):
    producer.produce(topic, value=data)
    producer.flush()

def main():
    data = generate_test_result()
    serialized = json.dumps(data, ensure_ascii=False).encode('utf-8')
    send_message('testing-topic', serialized)
    print('Отправлено сообщение:', json.dumps(data, ensure_ascii=False, indent=2))
    producer.close()
```

На скриншоте ниже – результат работы Producer'а: сгенерированное и отправленное сообщение выведено в консоль.

<img width="1920" height="1021" alt="image" src="https://github.com/user-attachments/assets/145762be-b645-4263-bef4-c5f790bb05a4" />

### 3. Разработка Consumer'а (потребителя данных)
Скрипт `consumer.py` выполняет:
- Подписку на топик `testing-topic`.
- Получение сообщений в бесконечном цикле.
- Декодирование и парсинг JSON.
- Валидацию сообщения с помощью функции `validate_message()`.
- Вывод результата валидации и содержимого сообщения в консоль.

Валидация включает:
- Проверку наличия обязательных полей.
- Проверку типов данных (числа, булевы значения).
- Логическую проверку: балл не может быть больше максимального, отрицательным.
- Соответствие статуса `is_passed` минимальному проходному баллу (60%).
- Проверку данных студента и теста.
- При наличии поля `percentage` – его соответствие вычисленному значению.

**Листинг `consumer.py`**:
```python
from confluent_kafka import Consumer, KafkaException
import json

def validate_message(data):
    # ... проверки ...
    return True, "Сообщение валидно"

def process_message(msg):
    data = json.loads(msg.value().decode('utf-8'))
    is_valid, msg_text = validate_message(data)
    status = "✓ ВАЛИДНОЕ" if is_valid else "✗ NOT VALID"
    print(f"\n{status}\n{json.dumps(data, indent=2, ensure_ascii=False)}")

def main():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'testing-system-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['testing-topic'])
    while True:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            process_message(msg)
```

Скриншот работы Consumer'а: полученное сообщение успешно прошло валидацию, выведены все детали.

<img width="1920" height="1021" alt="image" src="https://github.com/user-attachments/assets/a000069b-3b07-47d7-8e0c-eb5fd03f8b9b" />

### 4. Проверка работоспособности
Последовательность запуска:
1. Убедились, что Kafka запущена (см. первый скриншот).
2. Запустили Consumer: `python consumer.py`.
3. В другом терминале запустили Producer: `python producer.py`.
4. На стороне Consumer отобразилось полученное сообщение с подтверждением валидности.

Все шаги выполнены успешно, сообщение от Producer доставлено до Consumer, валидация пройдена.

## Выводы
В ходе лабораторной работы было развернуто окружение Apache Kafka, разработаны два приложения на Python: Producer для генерации и отправки тестовых сообщений и Consumer для приёма, валидации и отображения данных. Продемонстрирован полный цикл потоковой обработки: генерация → отправка → получение → валидация. Полученные навыки являются основой для построения систем обработки данных в реальном времени.

## Используемые материалы
- [Официальный сайт Apache Kafka](https://kafka.apache.org)
- [Библиотека confluent_kafka для Python](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/)
- [Статья на Habr: Consumer/Producer на Python](https://habr.com/ru/companies/otus/articles/789896/)
