FROM python:3.8.10

RUN apt update && apt install libgl1 libglib2.0-0 libsm6 libxrender1 libxext6 librdkafka-dev libgeos-dev -y 

RUN useradd -ms /bin/bash testuser
USER testuser
WORKDIR /home/testuser/code

COPY --chown=testuser:testuser . .

RUN pip install --user -r requirements.txt 

ENV PYTHONUNBUFFERED=1

CMD [ "python", "main.py" ]