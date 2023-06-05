import asyncio
from aiokafka import AIOKafkaConsumer
import websockets
from websockets.server import serve


async def kafka_consumer():
    consumer = AIOKafkaConsumer(
        "main",
        bootstrap_servers='127.0.0.1:9092',
        group_id="group1",
        enable_auto_commit=False
    )
    await consumer.start()

    try:
        async for msg in consumer:
            # Process the Kafka message
            print(f"Received message: {msg.value.decode()}")

            # Broadcast the Kafka message to all connected WebSocket clients
            await broadcast_message(msg.value.decode())

            # Commit the offset manually
            await consumer.commit()
    finally:
        await consumer.stop()

async def broadcast_message(message):
    # Send the message to all connected WebSocket clients
    for i in sockets:
        await i.send(message)

async def websocket_handler(websocket, path):
    # Add the new WebSocket client to the list
    sockets.add(websocket)
    try:
        # Keep the WebSocket connection open
        while True:
            await asyncio.sleep(1)
    finally:
        # Remove the WebSocket client from the list
        sockets.remove(websocket)

async def start_server():
    # Start the WebSocket server
    async with websockets.serve(websocket_handler, "localhost", 8765):
        await asyncio.Future()  # Keep the event loop running

if __name__ == "__main__":
    sockets = set()
    loop = asyncio.get_event_loop()
    loop.create_task(kafka_consumer())
    loop.run_until_complete(start_server())