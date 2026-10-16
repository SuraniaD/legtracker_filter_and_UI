FROM python:3.12-slim

# Unbuffered output so every progress line shows up in Railway's logs immediately.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY refresh.py transforms.py gap_models.py filter_job.py rules.py embeddings.py enrich_job.py ai_rules.py start.sh ./
COPY tracker_db ./tracker_db

# JOB picks the service: refresh (default), filter or enrich. See start.sh.
CMD ["sh", "start.sh"]