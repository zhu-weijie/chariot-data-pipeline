FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./src /app/src
COPY ./config /app/config
COPY ./main.py .

CMD ["tail", "-f", "/dev/null"]
