import cv2
import time
import os
import logging
from confluent_kafka import Producer

# from confluent_kafka.admin import AdminClient, NewTopic
# from confluent_kafka import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class ProcessStream:
    def __init__(self):
        self.topic_name = "video-to-frames"
        self.partition = int(os.environ.get("PARTITION", 0))
        self.producer = Producer(
            {
                "bootstrap.servers": "0.0.0.0:9094",
                "security.protocol": "PLAINTEXT",
                "compression.type": "snappy",
                "message.max.bytes": 5242880,
                "partitioner": "murmur2",
            }
        )
        # try:
        #     admin_client = AdminClient(
        #         {
        #             "bootstrap.servers": "0.0.0.0:9094",
        #             "security.protocol": "PLAINTEXT",
        #         }
        #     )

        #     topic = NewTopic(
        #         topic=self.topic_name, num_partitions=30, replication_factor=1
        #     )
        #     fs = admin_client.create_topics([topic])
        #     for topic_name, future in fs.items():
        #         try:
        #             future.result()
        #             logger.info(f"Topic {topic_name} created")
        #         except Exception as e:
        #             logger.warning(f"Failed to create topic {topic_name}: {e}")
        # except KafkaError as e:
        #     logger.warning(e)

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    def process_video(self, video_path, fps=25):
        try:
            cap = cv2.VideoCapture(video_path)

            if not cap.isOpened():
                logger.error(f"Error: failed to open video {video_path}")
                return

            original_fps = cap.get(cv2.CAP_PROP_FPS)

            frame_interval = max(1, int(original_fps / fps)) if original_fps > 0 else 1
            frame_count = 0
            sent_frames = 0

            while True:
                ret, frame = cap.read()

                if not ret:
                    logger.info("End of video reached")
                    break

                if frame_count % frame_interval == 0:
                    try:
                        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
                        _, buffer = cv2.imencode(".jpg", frame, encode_param)
                        self.producer.produce(
                            topic=self.topic_name,
                            key=f"camera_{self.partition}",
                            value=buffer.tobytes(),
                            callback=self._delivery_report,
                            partition=self.partition,
                        )
                        self.producer.flush()
                        time.sleep(1)
                        sent_frames += 1
                        logger.info(f"Queued frame {sent_frames}")

                    except Exception as e:
                        logger.error(f"Error processing frame {frame_count}: {str(e)}")

                frame_count += 1
            cap.release()

        except Exception as e:
            logger.error(f"Error processing video: {str(e)}")


if __name__ == "__main__":
    processStream = ProcessStream()
    processStream.process_video("./sample_04.mp4")
