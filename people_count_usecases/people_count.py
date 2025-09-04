import cv2
import numpy as np
import logging
import json
import os
import datetime
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
import mongoengine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class PeopleCountUsecase:
    def __init__(self):
        self.topic_name = "people-count"
        try:
            admin_client = AdminClient(
                {
                    "bootstrap.servers": "192.168.15.212:9094",
                    "security.protocol": "PLAINTEXT",
                }
            )

            topic = NewTopic(
                topic=self.topic_name, num_partitions=30, replication_factor=1
            )
            fs = admin_client.create_topics([topic])
            for topic_name, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic_name} created")
                except Exception as e:
                    logger.warning(f"Failed to create topic {topic_name}: {e}")
        except KafkaError as e:
            logger.warning(e)

        self.consumer = self.create_consumer()

        # self.consumer.subscribe([self.topic_name])
        self.consumer.assign([TopicPartition(self.topic_name, 0)])

        mongoengine.connect(
            os.getenv("DB_NAME"),
            host="mongodb://{}:{}@{}:{}".format(
                os.getenv("DB_USERNAME"),
                os.getenv("DB_PASSWORD"),
                os.getenv("SERVERIP"),
                os.getenv("DB_PORT"),
            ),
        )

        self.object_tracking = {}
        self.start_time = datetime.datetime.utcnow()

    def create_consumer(self):
        return Consumer(
            **{
                "bootstrap.servers": "192.168.15.212:9094",
                "security.protocol": "PLAINTEXT",
                "auto.offset.reset": "latest",
                "group.id": "consume-detection",
            }
        )

    def consume_from_source(self, topic, partition, offset):
        consumer = self.create_consumer()

        partition = TopicPartition(
            topic=topic,
            partition=int(partition),
            offset=int(offset),
        )

        consumer.assign([partition])
        while True:
            message = consumer.poll()
            frame = message.value()
            if message.offset() != offset:
                print(
                    f"*** IMAGE NOT FOUND (Offset reset to - {message.offset()} | Original offset - {offset}) ***"
                )
                frame = None

            consumer.close()
            return frame

    def process_data(self):
        if self.consumer:
            while True:
                message = self.consumer.poll(0.01)
                if message is None:
                    continue
                elif message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        print("Error occurred: End of partition")
                    else:
                        print("Error occurred: {}".format(message.error().str()))
                else:
                    data = json.loads(message.value().decode("utf-8"))
                    camera_id = message.key().decode("utf-8")
                    frame = self.consume_from_source(
                        topic="video-to-frames",
                        partition=message.partition(),
                        offset=data["offset"],
                    )
                    raw_bytes = np.frombuffer(frame, dtype=np.uint8)
                    raw_image = cv2.imdecode(raw_bytes, cv2.IMREAD_COLOR)

                    if message.key() not in self.object_tracking:
                        self.object_tracking[camera_id] = {
                            "person_ids": [],
                            "age": [],
                            "gender": [],
                        }
                    for detection in data["data"]:
                        cv2.rectangle(
                            raw_image,
                            (detection["cords"][0], detection["cords"][1]),
                            (detection["cords"][2], detection["cords"][3]),
                            (0, 255, 0),
                            2,
                        )
                        cv2.putText(
                            raw_image,
                            f'person_id: {detection["person_id"]}',
                            (detection["cords"][0], detection["cords"][1] - 10),
                            cv2.FONT_HERSHEY_SIMPLEX,
                            0.4,
                            (0, 255, 0),
                            2,
                        )
                        if (
                            detection["person_id"]
                            not in self.object_tracking[camera_id]["person_ids"]
                        ):
                            self.object_tracking[camera_id]["person_ids"].append(
                                detection["person_id"]
                            )
                            self.object_tracking[camera_id]["age"].append(
                                detection["age"]
                            )
                            self.object_tracking[camera_id]["gender"].append(
                                detection["gender"]
                            )
                    # if (datetime.datetime.utcnow() - self.start_time).seconds >= 5:
                    # logger.info(self.object_tracking)
                    info = self.object_tracking[camera_id]
                    person_ids = info["person_ids"]
                    ages = info["age"]
                    genders = info["gender"]

                    total_persons = len(person_ids)

                    avg_age = int(sum(ages) / len(ages)) if ages else 0

                    gender_counts = {}
                    for gender in genders:
                        gender_counts[gender] = gender_counts.get(gender, 0) + 1

                    gender_ratio = {
                        gender: f"{(count/total_persons)*100:.1f}%"
                        for gender, count in gender_counts.items()
                    }

                    print(f"{camera_id}:")
                    print(f"  Total persons: {total_persons}")
                    print(f"  Average age: {avg_age:.2f}")
                    print(f"  Gender ratio: {gender_ratio}")
                    print()

                    font = cv2.FONT_HERSHEY_SIMPLEX
                    font_scale = 0.4
                    color = (0, 0, 0)  # White
                    thickness = 1

                    cv2.putText(
                        raw_image,
                        f"{camera_id}:",
                        (10, 30),
                        font,
                        font_scale,
                        color,
                        thickness,
                    )
                    cv2.putText(
                        raw_image,
                        f"  Total persons: {total_persons}",
                        (10, 55),
                        font,
                        font_scale,
                        color,
                        thickness,
                    )
                    cv2.putText(
                        raw_image,
                        f"  Average age: {avg_age:.2f}",
                        (10, 80),
                        font,
                        font_scale,
                        color,
                        thickness,
                    )
                    cv2.putText(
                        raw_image,
                        f"  Gender ratio: {gender_ratio}",
                        (10, 105),
                        font,
                        font_scale,
                        color,
                        thickness,
                    )

                    cv2.imwrite(
                        os.path.join(os.getcwd(), "live_images", f"{camera_id}.png"),
                        raw_image,
                    )


peopleCountUsecase = PeopleCountUsecase()
peopleCountUsecase.process_data()
