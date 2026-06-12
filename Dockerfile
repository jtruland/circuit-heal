FROM python:3.13-slim

LABEL org.opencontainers.image.source=https://github.com/jtruland/circuit-heal

WORKDIR /app
RUN pip install --no-cache-dir docker==7.1.0
COPY circuit_heal.py .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "circuit_heal.py"]
