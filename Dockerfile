FROM python:3.11-slim
ARG APP_VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY komari_traffic_report.py /app/komari_traffic_report.py
COPY web_app.py /app/web_app.py
COPY static /app/static
RUN chmod +x /app/komari_traffic_report.py

LABEL org.opencontainers.image.title="komari-traffic-bot" \
      org.opencontainers.image.source="https://github.com/wirelouis/komari-traffic-bot" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"

ENV DATA_DIR=/data \
    APP_VERSION=${APP_VERSION} \
    GIT_COMMIT=${GIT_COMMIT} \
    BUILD_DATE=${BUILD_DATE} \
    IMAGE_SOURCE=ghcr.io/wirelouis/komari-traffic-bot
RUN useradd -m -u 10001 appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /data
VOLUME ["/data"]
USER appuser
CMD ["python", "/app/komari_traffic_report.py", "listen"]
