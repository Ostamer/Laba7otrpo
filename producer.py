import asyncio
import aiohttp
import os
import pika
from dotenv import load_dotenv
import sys
from utils import extract_links

load_dotenv()

QUEUE_NAME = os.getenv('QUEUE_NAME')
print("Producer script started.")

async def fetch_url(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.text()
        return ""

async def producer(start_url):
    print("Connecting to RabbitMQ...")
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

    visited = set()
    queue = [start_url]

    async with aiohttp.ClientSession() as session:
        while queue:
            url = queue.pop(0)
            if url in visited:
                continue

            print(f"Fetching: {url}")
            visited.add(url)

            html = await fetch_url(session, url)
            links = extract_links(html, url)

            for link in links:
                channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=link)
                print(f"Published: {link}")

            queue.extend(links - visited)

    connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python producer.py <start_url>")
        sys.exit(1)

    start_url = sys.argv[1]
    print(f"Starting producer with URL: {start_url}")

    asyncio.run(producer(start_url))
