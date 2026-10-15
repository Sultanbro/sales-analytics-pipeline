FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --create-home app && mkdir data && chown app:app data
COPY pipeline ./pipeline
USER app
EXPOSE 8010
CMD ["uvicorn", "pipeline.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8010"]
