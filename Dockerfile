FROM python:3.12-slim

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

RUN useradd --system --no-create-home --shell /usr/sbin/nologin teltonika

WORKDIR /opt/teltonika
COPY api_server.py ./

# Railway injects PORT itself; the default only matters for docker-compose.
ENV PORT=8000 \
    API_HOST=0.0.0.0 \
    API_LOG_LEVEL=INFO

USER teltonika
EXPOSE 8000

CMD ["python3", "-u", "/opt/teltonika/api_server.py"]
