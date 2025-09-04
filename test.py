import cv2
import uuid
import numpy as np
import datetime
from ultralytics import YOLO
from insightface.app import FaceAnalysis
from pymongo import MongoClient
from scipy.spatial.distance import cosine

# ---------------------------
# Configs
# ---------------------------
VIDEO_PATH = "/home/prem/Downloads/sample_04.mp4"
OUTPUT_PATH = "sample_04_out.mp4"
CAMERA_ID = "CAM_01"
SIMILARITY_THRESHOLD = 0.35  # lower = stricter matching

# ---------------------------
# MongoDB setup
# ---------------------------
client = MongoClient(
    "mongodb://192.168.1.123:27017/?readPreference=primary&directConnection=true&ssl=false"
)
db = client["person_counter"]
people_collection = db["people"]

# ---------------------------
# Load models
# ---------------------------
yolo = YOLO("yolov8n.pt")  # lightweight YOLOv8
face_app = FaceAnalysis(name="buffalo_l", providers=["CPUExecutionProvider"])
face_app.prepare(ctx_id=0, det_size=(640, 640))

# ---------------------------
# Video setup
# ---------------------------
cap = cv2.VideoCapture(VIDEO_PATH)
fps = int(cap.get(cv2.CAP_PROP_FPS))
w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

fourcc = cv2.VideoWriter_fourcc(*"mp4v")
out = cv2.VideoWriter(OUTPUT_PATH, fourcc, fps, (w, h))


# ---------------------------
# Helper Functions
# ---------------------------
def find_existing_person(embedding):
    """Check if embedding matches someone in DB"""
    all_people = list(people_collection.find())
    for person in all_people:
        stored_emb = np.array(person["embedding"], dtype=np.float32)
        sim = 1 - cosine(embedding, stored_emb)
        if sim > (1 - SIMILARITY_THRESHOLD):
            return person["person_id"]
    return None


def insert_new_person(embedding, gender, age):
    """Insert new unique person into DB"""
    person_id = str(uuid.uuid4())
    now = datetime.datetime.utcnow().isoformat()
    people_collection.insert_one(
        {
            "person_id": person_id,
            "embedding": embedding.tolist(),
            "gender": gender,
            "age": age,
            "camera_id": CAMERA_ID,
            "first_seen": now,
            "last_seen": now,
        }
    )
    return person_id


def update_person_last_seen(person_id):
    now = datetime.datetime.utcnow().isoformat()
    people_collection.update_one({"person_id": person_id}, {"$set": {"last_seen": now}})


def draw_stats(frame):
    """Draw statistics on frame"""
    total_unique = people_collection.count_documents({})
    males = people_collection.count_documents({"gender": "male"})
    females = people_collection.count_documents({"gender": "female"})

    cv2.putText(
        frame,
        f"Unique Count: {total_unique}",
        (20, 30),
        cv2.FONT_HERSHEY_SIMPLEX,
        1,
        (0, 255, 0),
        2,
    )
    cv2.putText(
        frame,
        f"M: {males} | F: {females}",
        (20, 60),
        cv2.FONT_HERSHEY_SIMPLEX,
        1,
        (0, 255, 0),
        2,
    )

    return frame


# ---------------------------
# Main Loop
# ---------------------------
while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    # Run YOLO person detection
    results = yolo.track(frame, classes=[0], persist=True)  # class 0 = person

    if results[0].boxes is not None:
        for box in results[0].boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            conf = float(box.conf[0])
            if conf < 0.5:
                continue

            # Crop person
            crop = frame[y1:y2, x1:x2]
            if crop.size == 0:
                continue

            # Face analysis
            faces = face_app.get(crop)
            if len(faces) > 0:
                face = faces[0]
                emb = face.normed_embedding
                gender = "male" if face.gender == 1 else "female"
                age = int(face.age)

                # Check DB
                existing_id = find_existing_person(emb)
                if existing_id:
                    person_id = existing_id
                    update_person_last_seen(person_id)
                else:
                    person_id = insert_new_person(emb, gender, age)

                # Draw bounding box
                cv2.rectangle(frame, (x1, y1), (x2, y2), (255, 0, 0), 2)
                cv2.putText(
                    frame,
                    f"ID:{person_id[:6]} {gender}, {age}",
                    (x1, y1 - 10),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    (255, 255, 0),
                    2,
                )

    # Draw stats overlay
    frame = draw_stats(frame)

    # Write frame
    out.write(frame)
    cv2.imshow("output", frame)

    if cv2.waitKey(1) & 0xFF == ord("q"):
        break

cap.release()
out.release()
cv2.destroyAllWindows()
