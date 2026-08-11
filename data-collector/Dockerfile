FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN groupadd --system collector \
    && useradd --system --gid collector --home-dir /app --no-create-home collector

COPY pyproject.toml README.md requirements.txt ./
COPY src ./src
COPY migrations ./migrations

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir .

USER collector

STOPSIGNAL SIGTERM
ENTRYPOINT ["python", "-m", "prediction_collector"]
CMD ["run", "--exchange", "all"]
