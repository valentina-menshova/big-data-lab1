import json
import time
from confluent_kafka import Producer

# конфигурация Producer'а
config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'testing-producer'
}
producer = Producer(config)

# Функция генерации данных о результате тестирования (одна посылка)
def generate_test_result():
    # Данные студента
    student = {
        'student_id': 12345,
        'first_name': 'Иван',
        'last_name': 'Петров',
        'group': 'ИТ-21',   
        'course': 2
    }
    
    # Данные теста
    test = {
        'test_id': 67890,
        'test_name': 'Тест по информатике',
        'max_score': 100
    }
    
    # Результат
    score = 85
    
    return {
        'result_id': 111222333,
        'timestamp': int(time.time()),
        'student': student,
        'test': test,
        'score': score,
        'max_score': 100,
        'percentage': 85,
        'is_passed': True
    }

# Функция сериализации данных
def serialize_data(data):
    return json.dumps(data, ensure_ascii=False).encode('utf-8')

# Функция отправки сообщения
def send_message(topic, data):
    try:
        producer.produce(topic, value=data)
        producer.flush()
        return True
    except Exception as e:
        print(f"Ошибка отправки: {e}")
        return False

# Основная функция
def main():
    # Генерируем одно сообщение
    data = generate_test_result()
    
    # Сериализуем данные
    serialized_data = serialize_data(data)
    
    # Отправляем в Kafka
    if send_message('testing-topic', serialized_data):
        print('Отправлено сообщение:')
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print('-' * 50)
    else:
        print('Ошибка при отправке сообщения')
    
    producer.close()

if __name__ == "__main__":
    main()