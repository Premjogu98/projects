import numpy as np
import logging
from confluent_kafka import Consumer, KafkaError, TopicPartition
from ultralytics import YOLO
from insightface.app import FaceAnalysis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class ProcessFrames:
    def __init__(self):
        self.topic_name = "video-to-frames"
        self.consumer = Consumer(
            **{
                "bootstrap.servers": "192.168.1.123:9094,192.168.1.123:9095,192.168.1.123:9096",
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.username": "admin",
                "sasl.password": "admin-secret",
                "sasl.mechanism": "PLAIN",
                "auto.offset.reset": "latest",
                "group.id": "consume-frames",
            }
        )
        self.consumer.assign([TopicPartition(self.topic_name, 0)])
        self.yolo = YOLO("yolov8n.pt")

        # self.face_app = FaceAnalysis(
        #     name="buffalo_l", providers=["CPUExecutionProvider"]
        # )
        # self.face_app.prepare(ctx_id=0, det_size=(640, 640))

    def consume_data(self):
        if self.consumer:
            while True:
                message = self.consumer.poll(0.01)
                logger.debug(message)
                if message is None:
                    continue
                elif message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        print("Error occurred: End of partition")
                    else:
                        print("Error occurred: {}".format(message.error().str()))
                else:
                    raw_image = np.frombuffer(message.value(), dtype=np.uint8)
                    results = self.yolo.track(raw_image, classes=[0], persist=True)

                    if results[0].boxes is not None:
                        for box in results[0].boxes:
                            x1, y1, x2, y2 = map(int, box.xyxy[0])
                            conf = float(box.conf[0])
                            if conf < 0.5:
                                continue

                            crop = raw_image[y1:y2, x1:x2]
                            if crop.size == 0:
                                continue
                            faces = self.face_app.get(crop)
                            if len(faces) > 0:
                                face = faces[0]
                                emb = face.normed_embedding
                                gender = "male" if face.gender == 1 else "female"
                                age = int(face.age)
                                logger.debug((emb, gender, age))


processFrames = ProcessFrames()
processFrames.consume_data()
