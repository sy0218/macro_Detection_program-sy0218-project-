#!/usr/bin/python3
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
import os
import sys
import concurrent.futures

def consumer_partition(servers, topic, partition, output_file, consum_group):
    # 파티션에 따른 컨슈머 생성
    consumer = KafkaConsumer(
        bootstrap_servers=servers,
        group_id=consum_group, # 컨슈머 그룹 id
        auto_offset_reset='earliest', # 처음부터 메시지 읽음
        enable_auto_commit=False, # 자동 커밋 안함
        consumer_timeout_ms=1000 # 컨슈머가 새로운 메시지를 기다리는 시간을 1초로 지정
    )
    partition = TopicPartition(topic, partition)
    # 컨슈머 특정 파티션 할당
    consumer.assign([partition])

    # 파티션 오프셋 처음으로 설정
    consumer.seek_to_beginning(partition)

    with open(output_file, 'a') as f:
        for message in consumer:
            f.write(f"{message.value.decode('utf-8')}\n")


def main(servers, topic, output_file, consum_group):
    # 디렉토리 미 존재시 생성
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # 토픽 파티션 수를 알아내기 위한 api 호출
    admin_client = KafkaAdminClient(bootstrap_servers=servers) # api 호출을 위한 객체 생성
    topic_metadata = admin_client.describe_topics([topic])[0]
    partitions = topic_metadata.get('partitions', [])
    print(partitions)

    # 병렬 처리
    with concurrent.futures.ThreadPoolExecutor() as executor:
        consumer_jobs = []
        for partition_info in partitions:
            partition = partition_info['partition']
            # 특정 함수 비동기 실행
            # consumer_partition 함수 실행
            # 함수 실행 인자 servers, topic, partition, output_file
            consumer_job = executor.submit(consumer_partition, servers, topic, partition, output_file, consum_group)
            consumer_jobs.append(consumer_job)

        # 모든 스레드 완료시까지 기다림
        concurrent.futures.wait(consumer_jobs)


if __name__ == "__main__":
    script_name = os.path.basename(__file__)

    args = sys.argv[1:]
    # 인자가 3개가 아니면 종료
    if len(args) != 4:
        print(f"사용법: {script_name} <bootstrap_servers> <topic> <output_file> <consum_group>")
        sys.exit(1)

    # 각 인자를 변수로 지정
    servers, topic, output_file, consum_group = args

    # output_file이 존재하면 삭제하여 멱등성 보장
    if os.path.exists(output_file):
        os.remove(output_file)

    # main 함수 호출
    main(servers, topic, output_file, consum_group)
