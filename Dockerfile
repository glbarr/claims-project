FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV OPENSSL_CONF=/app/openssl_legacy.cnf

WORKDIR /app

# Copy OpenSSL config, project definition, and minimal package structure
COPY openssl_legacy.cnf ./
COPY pyproject.toml ./
COPY src/claims_processor/__init__.py src/claims_processor/__init__.py

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .[dev]

COPY . .

RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "claims_processor.main"]