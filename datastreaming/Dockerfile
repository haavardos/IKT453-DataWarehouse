FROM python:3.11-slim
WORKDIR /app
COPY consumer_mongo.py .
RUN pip install kafka-python pymongo
CMD ["python", "-u", "consumer_mongo.py"]
