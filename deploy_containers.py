import docker
import os
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError

admin_client = AdminClient(
    {
        "bootstrap.servers": "0.0.0.0:9094",
        "security.protocol": "PLAINTEXT",
    }
)
try:
    topic = NewTopic(topic="video-to-frames", num_partitions=30, replication_factor=1)
    topic1 = NewTopic(topic="people-count", num_partitions=30, replication_factor=1)
    fs = admin_client.create_topics([topic, topic1])
    for topic_name, future in fs.items():
        try:
            future.result()
            print(f"Topic {topic_name} created")
        except Exception as e:
            print(f"Failed to create topic {topic_name}: {e}")
except KafkaError as e:
    print(e)


client = docker.from_env()

# # ============= Deploy Video Stream ============= #
partitions = []
ai_container_counter = 1
for camera_id in range(3):
    container_config = {
        "image": "premjogu98/registry:process-video-0.0.1",
        "network_mode": "host",
        "environment": {"PARTITION": str(camera_id)},
        # "volumes": {os.getcwd(): {"bind": "/code", "mode": "rw"}},
        "detach": True,
        "remove": True,
        "name": f"camera_{camera_id}",
    }

    try:
        # Run the container
        print(f"Starting container from image: {container_config['image']}")
        container = client.containers.run(**container_config)

        # If detach=True, you can interact with the container
        print(f"Container ID: {container.id}")
        print(f"Container status: {container.status}")

    except docker.errors.ImageNotFound:
        print(f"Error: Image '{container_config['image']}' not found")
        print("Please ensure the image is built or pulled before running")
    except docker.errors.APIError as e:
        print(f"Docker API error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    partitions.append(camera_id)

    if len(partitions) == 3:
        container_config = {
            "image": "premjogu98/registry:inference_video-0.0.1",
            "network_mode": "host",
            "environment": {"PARTITIONS": ",".join(partitions)},
            "detach": True,
            "remove": True,
            "name": f"Ai_Model_{ai_container_counter}",
        }
        ai_container_counter += 1
        partitions.clear()


# ============= Deploy Usecase ============= #
container_config = {
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
    "name": f"Person_Count",
}

try:
    # Run the container
    print(f"Starting container from image: {container_config['image']}")
    container = client.containers.run(**container_config)

    # If detach=True, you can interact with the container
    print(f"Container ID: {container.id}")
    print(f"Container status: {container.status}")

except docker.errors.ImageNotFound:
    print(f"Error: Image '{container_config['image']}' not found")
    print("Please ensure the image is built or pulled before running")
except docker.errors.APIError as e:
    print(f"Docker API error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
