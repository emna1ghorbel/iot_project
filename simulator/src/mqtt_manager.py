import logging
import json
from awscrt import mqtt
from awsiot import mqtt_connection_builder

log = logging.getLogger(__name__)

class MQTTManager:
    def __init__(self, endpoint, cert_file, key_file, ca_file, client_id):
        self.endpoint = endpoint
        self.cert_file = cert_file
        self.key_file = key_file
        self.ca_file = ca_file
        self.client_id = client_id
        self.connection = None

    def connect(self, on_interrupted=None, on_resumed=None):
        log.info(f"Connecting to AWS IoT at {self.endpoint} as {self.client_id}...")
        
        self.connection = mqtt_connection_builder.mtls_from_path(
            endpoint=self.endpoint,
            cert_filepath=self.cert_file,
            pri_key_filepath=self.key_file,
            ca_filepath=self.ca_file,
            client_id=self.client_id,
            clean_session=False,
            keep_alive_secs=30,
            on_connection_interrupted=on_interrupted,
            on_connection_resumed=on_resumed,
        )

        connect_future = self.connection.connect()
        # Future.result() waits until a result is available
        connect_future.result()
        log.info("MQTT Connected successfully!")

    def subscribe(self, topic, qos=mqtt.QoS.AT_LEAST_ONCE, callback=None):
        if not self.connection:
            raise RuntimeError("MQTT Connection not established. Call connect() first.")
        
        log.info(f"Subscribing to topic: {topic}")
        subscribe_future, packet_id = self.connection.subscribe(
            topic=topic,
            qos=qos,
            callback=callback
        )
        subscribe_future.result()
        log.info(f"Subscribed to {topic} with QoS {qos}")

    def publish(self, topic, payload, qos=mqtt.QoS.AT_LEAST_ONCE):
        if not self.connection:
            raise RuntimeError("MQTT Connection not established. Call connect() first.")
        
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        
        self.connection.publish(
            topic=topic,
            payload=payload,
            qos=qos
        )
        log.debug(f"Published message to {topic}")

    def disconnect(self):
        if self.connection:
            log.info("Disconnecting from MQTT...")
            disconnect_future = self.connection.disconnect()
            disconnect_future.result()
            log.info("Disconnected.")
