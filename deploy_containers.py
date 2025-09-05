import docker
import os
import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError
from docker.types import DeviceRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class DockerManager:
    def __init__(self):
        self.client = docker.from_env()

    def __call__(self):
        self.deploy_video_stream()
        self.deploy_usecase()

    def run_container(self, config):
        try:
            logger.info(f"Starting container from image: {config['image']}")
            container = self.client.containers.run(**config)
            logger.info(
                f"Container ID: {container.id} | Container status: {container.status}"
            )
            return container
        except Exception as e:
            logger.info(e)
        return None

    def deploy_video_stream(self, num_cameras=3, partitions_per_ai=3):
        partitions = []
        ai_container_counter = 1
        for camera_id in range(num_cameras):
            config = {
                "image": "premjogu98/registry:process-video-0.0.1",
                "network_mode": "host",
                "environment": {"PARTITION": str(camera_id)},
                "detach": True,
                "remove": True,
                "name": f"camera_{camera_id}",
            }
            self.run_container(config)
            partitions.append(str(camera_id))

            if len(partitions) == partitions_per_ai:
                ai_config = {
                    "image": "premjogu98/registry:inference_video-0.0.1",
                    "network_mode": "host",
                    "environment": {"PARTITIONS": ",".join(partitions)},
                    "detach": True,
                    "remove": True,
                    "name": f"Ai_Model_{ai_container_counter}",
                    "device_requests": [
                        DeviceRequest(
                            count=-1,  # Use -1 for all GPUs; change to a positive number (e.g., 1) for a specific count
                            capabilities=[["gpu"]],
                        )
                    ],
                }
                self.run_container(ai_config)
                ai_container_counter += 1
                partitions.clear()

    def deploy_usecase(self):
        config = {
            "image": "premjogu98/registry:person_count-0.0.1",
            "network_mode": "host",
            "volumes": {
                os.path.join(os.getcwd(), "live_images"): {
                    "bind": "/home/testuser/code/live_images",
                    "mode": "rw",
                }
            },
            "detach": True,
            "remove": True,
            "name": "Person_Count",
        }
        self.run_container(config)


if __name__ == "__main__":
    admin_client = AdminClient(
        {
            "bootstrap.servers": "0.0.0.0:9094",
            "security.protocol": "PLAINTEXT",
        }
    )
    topics = [
        {"name": "video-to-frames", "partitions": 30, "replication": 1},
        {"name": "people-count", "partitions": 30, "replication": 1},
    ]
    new_topics = [
        NewTopic(
            topic=config["name"],
            num_partitions=config["partitions"],
            replication_factor=config["replication"],
        )
        for config in topics
    ]
    try:
        futures = admin_client.create_topics(new_topics)
        for topic_name, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic_name} created")
            except Exception as e:
                logger.info(f"Failed to create topic {topic_name}: {e}")
    except KafkaError as e:
        logger.info(e)

    docker_manager = DockerManager()
    docker_manager()
