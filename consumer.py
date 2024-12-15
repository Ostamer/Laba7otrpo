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
    await asyncio.sleep(1)

async def consume():
    """
    Основной асинхронный потребитель, читающий сообщения из очереди RabbitMQ.
    """
    loop = asyncio.get_event_loop()
    message_queue = asyncio.Queue()

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
        loop.call_soon_threadsafe(message_queue.put_nowait, url)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=True)

    async def process_messages():
        while True:
            url = await message_queue.get()
            await process_url(url)

    try:
        await asyncio.gather(
            process_messages(),
            loop.run_in_executor(None, channel.start_consuming)
        )
    except KeyboardInterrupt:
        print("Stopping consumer...")
        channel.stop_consuming()
    finally:
        connection.close()

asyncio.run(consume())
