import pika
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

QUEUE_NAME = os.getenv('QUEUE_NAME')


async def process_url(url):
    """
    Обрабатывает URL.
    """
    print(f"Processing {url}")
    # Здесь добавьте код для обработки URL, например, повторную загрузку ссылок или другую логику.
    await asyncio.sleep(1)  # Замените на вашу логику.


async def consume():
    """
    Основной асинхронный потребитель, читающий сообщения из очереди RabbitMQ.
    """
    loop = asyncio.get_event_loop()

    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST'),
        port=int(os.getenv('RABBITMQ_PORT')),
        credentials=pika.PlainCredentials(
            os.getenv('RABBITMQ_USER'),
            os.getenv('RABBITMQ_PASSWORD')
        )
    ))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print("Connected to RabbitMQ. Waiting for messages...")

    def on_message(ch, method, properties, body):
        """
        Обработчик для каждого сообщения из RabbitMQ.
        """
        url = body.decode()
        print(f"Consumed: {url}")
        loop.create_task(process_url(url))  # Запускаем асинхронную обработку

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=True)

    try:
        channel.start_consuming()  # Запуск основного цикла потребления сообщений
    except KeyboardInterrupt:
        print("Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()

asyncio.run(consume())
