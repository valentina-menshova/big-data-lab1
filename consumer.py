from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_message(data):
    """Валидация сообщения с результатами тестирования"""
    try:
        # Проверка наличия обязательных полей верхнего уровня
        required_root_fields = ['result_id', 'timestamp', 'student', 'test', 'score', 'max_score', 'is_passed']
        for field in required_root_fields:
            if field not in data:
                return False, f"Отсутствует обязательное поле: {field}"
        
        # Проверка типов данных
        if not isinstance(data['result_id'], int):
            return False, "result_id должен быть числом"
        
        if not isinstance(data['score'], (int, float)):
            return False, "score должен быть числом"
        
        if not isinstance(data['max_score'], (int, float)):
            return False, "max_score должен быть числом"
        
        if not isinstance(data['is_passed'], bool):
            return False, "is_passed должен быть булевым значением"
        
        # Проверка логики оценки
        if data['score'] < 0 or data['score'] > data['max_score']:
            return False, f"Некорректная оценка: {data['score']} (максимум {data['max_score']})"
        
        # Проверка соответствия is_passed и score
        min_passing_score = data['max_score'] * 0.6
        expected_passed = data['score'] >= min_passing_score
        if data['is_passed'] != expected_passed:
            return False, f"Несоответствие статуса сдачи: is_passed={data['is_passed']} при оценке {data['score']}/{data['max_score']}"
        
        # Проверка данных студента
        student = data.get('student', {})
        required_student_fields = ['student_id', 'first_name', 'last_name', 'group']
        for field in required_student_fields:
            if field not in student:
                return False, f"В данных студента отсутствует поле: {field}"
        
        if not isinstance(student['student_id'], int):
            return False, "student_id должен быть числом"
        
        # Проверка данных теста
        test = data.get('test', {})
        required_test_fields = ['test_id', 'test_name']
        for field in required_test_fields:
            if field not in test:
                return False, f"В данных теста отсутствует поле: {field}"
        
        if not isinstance(test['test_id'], int):
            return False, "test_id должен быть числом"
        
        # Дополнительная проверка: если есть percentage, проверим его соответствие
        if 'percentage' in data:
            expected_percentage = round((data['score'] / data['max_score']) * 100, 2)
            if abs(data['percentage'] - expected_percentage) > 0.01:  # Допускаем небольшую погрешность
                return False, f"Несоответствие percentage: {data['percentage']} vs {expected_percentage}"
        
        return True, "Сообщение валидно"
        
    except Exception as e:
        return False, f"Ошибка при валидации: {str(e)}"

def process_message(msg):
    """Обработка полученного сообщения"""
    try:
        # Декодируем и парсим JSON
        message_value = msg.value().decode('utf-8')
        data = json.loads(message_value)
        
        print("\n" + "="*60)
        print(f"Получено сообщение:")
        print(f"Топик: {msg.topic()}")
        print(f"Партиция: {msg.partition()}")
        print(f"Смещение: {msg.offset()}")
        print(f"Ключ: {msg.key()}")
        print("-" * 60)
        
        # Валидация сообщения
        is_valid, validation_message = validate_message(data)
        
        if is_valid:
            print("✓ СТАТУС: ВАЛИДНОЕ СООБЩЕНИЕ")
            print("\nСодержимое:")
            print(json.dumps(data, ensure_ascii=False, indent=2))
            
            # Дополнительная информация для наглядности
            student = data.get('student', {})
            test = data.get('test', {})
            print(f"\n📊 Результат: {data.get('score')}/{data.get('max_score')} ({data.get('percentage', 0)}%)")
            print(f"👨‍🎓 Студент: {student.get('last_name')} {student.get('first_name')} (Группа: {student.get('group')})")
            print(f"📝 Тест: {test.get('test_name')}")
            print(f"✅ Сдал: {'Да' if data.get('is_passed') else 'Нет'}")
        else:
            print("✗ СТАТУС: НЕ ВАЛИДНОЕ СООБЩЕНИЕ")
            print(f"Причина: {validation_message}")
            print("\nПолученные данные:")
            print(json.dumps(data, ensure_ascii=False, indent=2))
        
        print("="*60)
        
    except json.JSONDecodeError as e:
        print(f"\n✗ ОШИБКА: Не удалось декодировать JSON - {str(e)}")
        print(f"Полученные сырые данные: {message_value}")
    except Exception as e:
        print(f"\n✗ ОШИБКА при обработке сообщения: {str(e)}")

def main():
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'testing-system-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'max.poll.interval.ms': 300000
    }
    
    # Создаем consumer
    consumer = Consumer(kafka_config)
    topic = 'testing-topic'  # Используем правильное название топика
    
    try:
        consumer.subscribe([topic])
        print(f"Consumer запущен. Подписка на топик: {topic}")
        print("Ожидание сообщений... (Нажмите Ctrl+C для выхода)")
        print("="*60)
        
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Достигнут конец партиции - это нормально
                    sys.stderr.write(f'Достигнут конец партиции {msg.topic()} [{msg.partition()}] на смещении {msg.offset()}\n')
                else:
                    # Другая ошибка
                    raise KafkaException(msg.error())
            else:
                # Успешно получили сообщение
                process_message(msg)
                
    except KeyboardInterrupt:
        print("\n\nПолучен сигнал остановки. Завершение работы...")
    except Exception as e:
        print(f"\nКритическая ошибка: {str(e)}")
        raise
    finally:
        # Закрываем consumer
        consumer.close()
        print("Consumer остановлен.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()