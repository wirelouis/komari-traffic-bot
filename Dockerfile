FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests

ADD https://github.com/aptible/supercronic/releases/download/v0.2.29/supercronic-linux-amd64 /usr/local/bin/supercronic
RUN chmod +x /usr/local/bin/supercronic

COPY komari_traffic_report.py /app/komari_traffic_report.py
RUN chmod +x /app/komari_traffic_report.py

ENV DATA_DIR=/data
RUN useradd -m -u 10001 appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /data
VOLUME ["/data"]
USER appuser
CMD ["python", "/app/komari_traffic_report.py", "listen"]
