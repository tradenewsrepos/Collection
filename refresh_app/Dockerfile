FROM python:3.8-slim-buster

WORKDIR /refresh_app
COPY  . .
RUN mkdir /logs
RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc-dev \
    unixodbc \
    libpq-dev \
    gcc \
    g++
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
CMD ["python", "-u", "main.py"]