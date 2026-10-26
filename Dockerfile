FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY rlg ./rlg
RUN pip install --no-cache-dir . && useradd -u 10001 -m gw
USER 10001
EXPOSE 8080
CMD ["uvicorn", "rlg.gateway:app", "--host", "0.0.0.0", "--port", "8080", "--no-access-log"]
