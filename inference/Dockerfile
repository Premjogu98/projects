FROM nvcr.io/nvidia/tensorrt:23.10-py3

RUN apt-get update && apt install libgl1 libglib2.0-0 libsm6 libxrender1 libxext6 librdkafka-dev libgeos-dev -y && pip3 install --upgrade pip

RUN useradd -ms /bin/bash testuser
USER testuser
WORKDIR /home/testuser/code

COPY --chown=testuser:testuser . .

RUN pip install --user -r requirements.txt 

ENV PYTHONUNBUFFERED=1

CMD [ "python", "process_frames.py" ]