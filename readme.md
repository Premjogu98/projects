# 👥 People Counting & Demographics Pipeline

This repository implements a **Kafka-based video analytics pipeline**.
It processes video streams to **detect people, track them, estimate age & gender**, and produces **live annotated snapshots with statistics** while also saving aggregated statistics into **MongoDB**.

---

## 🐳 Pull Docker Images First

```bash
# Media Server (Capture Image From Video)
docker pull premjogu98/registry:process-video-0.0.1

# Processing (Inference on Image)
docker pull premjogu98/registry:inference_video-0.0.1

# Usecase Logic (Final People Count Logic)
docker pull premjogu98/registry:person_count-0.0.1
```

---

## Overview

The pipeline has **three main stages**:

### 1. 🎥 Frame Producer (`capture_frames.py`)

- Reads video files or streams.
- Encodes frames into JPEG.
- Publishes them to Kafka (**`video-to-frames`** topic).

### 2. 🧠 Frame Processor (`process_frames.py`)

- Consumes frames from Kafka.
- Runs **YOLOv8 + ByteTrack** for people detection & tracking.
- Runs **InsightFace** for age & gender estimation.
- Publishes detection metadata (person ID, bounding box, age, gender) to Kafka (**`people-count`** topic).

### 3. 📊 People Counting & Analytics (`people_count.py`)

- Consumes detection metadata and retrieves original frames from Kafka.
- Draws bounding boxes and labels (ID, age, gender).
- Aggregates and logs statistics:

  - **Total number of people**
  - **Average age**
  - **Gender ratio**

- Saves annotated frames into `live_images/`.
- Every **5 seconds**, inserts a new entry into **MongoDB** with the latest stats for each camera (`CameraStats` collection).

---

## ⚙️ Tech Stack

- **Apache Kafka** → Messaging backbone
- **OpenCV** → Frame processing & visualization
- **YOLOv8** → People detection & tracking
- **ByteTrack** → Multi-object tracking
- **InsightFace** → Age & gender estimation
- **MongoDB + MongoEngine** → Persistent storage of stats
- **Python** → Glue logic
- **Docker** → Containerized deployment

---

## 🚀 How It Works

There are **two ways to run this pipeline**:

### 🔹 Local Python Execution

1. **Start Kafka** (with Zookeeper & Brokers).

2. **Start MongoDB** (local or container).

3. **Run Frame Producer**:

   ```bash
   python capture_frames.py
   ```

4. **Run Frame Processor(s)**:

   ```bash
   PARTITIONS=0,1,2 python process_frames.py
   ```

5. **Run People Counter**:

   ```bash
   python people_count.py
   ```

   - Saves annotated frames in `live_images/`.
   - Inserts aggregated stats into MongoDB every 5 seconds.

---

### 🔹 Dockerized Execution

We provide **Docker Compose + Deployment Script** for running the full pipeline:

- `docker-compose-db.yml` → Spins up MongoDB.
- `docker-compose-kafka.yml` → Spins up **Kafka & Zookeeper**.
- `deploy_containers.py` → Automatically:

  - Creates Kafka topics: `video-to-frames` & `people-count`.
  - Deploys multiple **video stream containers** (`process-video-0.0.1`).
  - Groups partitions (3 at a time) and launches an **AI inference container** (`inference_video-0.0.1`).
  - Deploys the **people counting container** (`person_count-0.0.1`) which also saves stats to MongoDB.

#### Steps:

```bash
# Start Kafka & dependencies
docker-compose -f docker-compose-kafka.yml up -d

# Start MongoDB
docker-compose -f docker-compose-db.yml up -d

# Deploy processing containers
python deploy_containers.py
```

---

## 📊 Example Output

### Console Logs

```text
camera_0:
  Total persons: 5
  Average age: 32
  Gender ratio: {'male': '60.0%', 'female': '40.0%'}
```

### MongoDB Entry Example

```json
{
  "camera_id": "camera_0",
  "person_ids": [1, 2, 3],
  "ages": [25, 28, 28],
  "genders": ["male", "female", "male"],
  "total_persons": 3,
  "avg_age": 28,
  "gender_ratio": { "male": "66.7%", "female": "33.3%" },
  "timestamp": "2025-09-05T12:34:56Z"
}
```

### Annotated Images

- Bounding boxes with person ID, Age, and Gender.
- Saved under `live_images/`.

## 🔧 Environment Variables

MongoDB connection is configured via `.env` file:

```
DB_NAME=your_db_name
DB_USERNAME=your_user
DB_PASSWORD=your_password
SERVERIP=localhost
DB_PORT=27017
```

Kafka environment variables:

- `PARTITION` → Kafka partition index (used by video producer containers).
- `PARTITIONS` → Comma-separated partitions for AI inference containers (e.g., `0,1,2`).

---

## ✅ Use Cases

- Real-time **people counting**.
- **Age & gender demographics** analysis.
- **Retail analytics**, **smart surveillance**, **event monitoring**.
- Persistent storage of people stats in **MongoDB** for further analytics and dashboards.

## 🗂️ Resource Stats Report

There is a folder named `resource_stats_report` included in the repository.
It contains snapshots of GPU, CPU, and Docker container statistics reports.

Please refer to this folder for detailed resource usage insights.

**Note:** The pipeline may use a high amount of CPU resources due to the combination of threading and OpenCV operations, especially while writing each image. This is expected behavior given the real-time processing requirements.

---

💡 _Tip: Use the Dockerized workflow for faster startup and easy scaling._

---
