FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0\
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash testuser
USER testuser

WORKDIR /home/testuser/code

COPY --chown=testuser:testuser requirements.txt .
RUN pip install -r requirements.txt

COPY --chown=testuser:testuser . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "people_count.py"]