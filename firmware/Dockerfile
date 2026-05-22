FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir paho-mqtt
COPY mock_sensor.py .
CMD ["python", "mock_sensor.py"]
