import cv2
import os
import numpy as np
import logging
import json
from confluent_kafka import Consumer, KafkaError, TopicPartition
from ultralytics import YOLO
from confluent_kafka import Producer
from insightface.app import FaceAnalysis
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class ProcessFrames:
    def __init__(self):
        self.usecase_topic = "people-count"

        self.producer = Producer(
            {
                "bootstrap.servers": "0.0.0.0:9094",
                "security.protocol": "PLAINTEXT",
                "compression.type": "snappy",
                "message.max.bytes": 5242880,
                "partitioner": "murmur2",
            }
        )

        self.yolo = YOLO("yolov8n.pt")

        self.face_app = FaceAnalysis(
            name="buffalo_l", providers=["CPUExecutionProvider"]
        )
        self.face_app.prepare(ctx_id=0, det_size=(640, 640))

    def consume_data(self, partition):
        consumer = Consumer(
            **{
                "bootstrap.servers": "0.0.0.0:9094",
                "security.protocol": "PLAINTEXT",
                "auto.offset.reset": "latest",
                "group.id": "consume-frames",
            }
        )
        consumer.assign([TopicPartition("video-to-frames", int(partition))])
        logger.info(f"Started consuming partition {partition}")
        if consumer:
            while True:
                message = consumer.poll(0.01)
                if message is None:
                    continue
                elif message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        print("Error occurred: End of partition")
                    else:
                        print("Error occurred: {}".format(message.error().str()))
                else:
                    try:
                        raw_bytes = np.frombuffer(message.value(), dtype=np.uint8)
                        raw_image = cv2.imdecode(raw_bytes, cv2.IMREAD_COLOR)
                        results = self.yolo.track(
                            raw_image,
                            classes=[0],
                            persist=True,
                            tracker="bytetrack.yaml",
                            conf=0.35,
                        )
                        detections = []
                        if results[0].boxes is not None:
                            for box in results[0].boxes:
                                x1, y1, x2, y2 = map(int, box.xyxy[0])
                                conf = float(box.conf[0])
                                if conf < 0.4:
                                    continue
                                crop = raw_image[y1:y2, x1:x2]
                                if crop.size == 0:
                                    continue

                                faces = self.face_app.get(crop)
                                if len(faces) > 0:
                                    face = faces[0]
                                    gender = "male" if face.gender == 1 else "female"
                                    age = int(face.age)
                                    detection = {
                                        "person_id": int(box.id[0]),
                                        "gender": gender,
                                        "age": age,
                                        "cords": [x1, y1, x2, y2],
                                    }
                                    # logger.info((partition, detection))
                                    detections.append(detection)
                        result = {
                            "data": detections,
                            "offset": message.offset(),
                        }
                        logger.info(
                            f"Partition: {partition} / {message.partition()} | {result} "
                        )
                        self.producer.produce(
                            topic=self.usecase_topic,
                            key=message.key(),
                            value=json.dumps(
                                {
                                    "data": detections,
                                    "offset": message.offset(),
                                }
                            ),
                            partition=message.partition(),
                        )
                        self.producer.flush()
                    except Exception as e:
                        logger.error(f"Error in partition {partition}: {e}")


if __name__ == "__main__":
    processFrames = ProcessFrames()
    partitions = []
    thread = []
    for part in os.environ.get("PARTITIONS", "0,1,2").split(","):
        t = threading.Thread(target=processFrames.consume_data, args=(part,))
        t.start()
        thread.append(t)

    for t in thread:
        t.join()
