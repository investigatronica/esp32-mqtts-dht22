# based on https://github.com/peterhinch/micropython-mqtt/tree/master/mqtt_as

from mqtt_as import MQTTClient
from config import config
import uasyncio as asyncio
from machine import Pin
import dht, json

d = dht.DHT22(Pin(13))

def sub_cb(topic, msg, retained):
    c, r = [int(x) for x in msg.decode().split(' ')]
    print('Topic = {} Count = {} Retransmissions = {} Retained = {}'.format(topic.decode(), c, r, retained))

async def wifi_han(state):
    print('Wifi is ', 'up' if state else 'down')
    await asyncio.sleep(1)

# If you connect with clean_session True, must re-subscribe (MQTT spec 3.1.2.4)
async def conn_han(client):
    await client.subscribe('result', 1)

async def main(client):
    await client.connect()
    n = 0
    await asyncio.sleep(2)  # Give broker time
    while True:
        print('publish', n)
        # If WiFi is down the following will pause for the duration.
        try:
            d.measure()
            try:
                temperatura=d.temperature()
                try:
                    humedad=d.humidity()
                    datos=json.dumps({
                        'client_id':config['client_id'],
                        'temperatura':'{:4.2f}'.format(temperatura),
                        'humedad':'{:3.1f}'.format(humedad)
                    })
                    await client.publish('sensor', datos, qos = 1)
                    n += 1
                except OSError as e:
                    print("sin sensor")
            except OSError as e:
                print("sin sensor")
        except OSError as e:
            print("sin sensor")
        await asyncio.sleep(20)  # Broker is slow

# Define configuration
config['subs_cb'] = sub_cb
config['connect_coro'] = conn_han
config['wifi_coro'] = wifi_han
config['ssl'] = True
config['port'] = 8883

# Set up client
MQTTClient.DEBUG = True  # Optional
client = MQTTClient(config)
try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()
