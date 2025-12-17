FROM python:3.11-slim

# System deps (ODBC + build essentials for some python wheels)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg2 apt-transport-https ca-certificates unixodbc unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# (Often required for SQL Server) Microsoft ODBC Driver 18 install
# apt-key is not available on newer Debian (e.g. trixie). Use keyring instead.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl gnupg ca-certificates apt-transport-https; \
    mkdir -p /etc/apt/keyrings; \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg; \
    chmod go+r /etc/apt/keyrings/microsoft.gpg; \
    curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
      | sed 's|^deb |deb [signed-by=/etc/apt/keyrings/microsoft.gpg] |' \
      > /etc/apt/sources.list.d/mssql-release.list; \
    apt-get update; \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18; \
    rm -rf /var/lib/apt/lists/*

# Install dbt + adapter (pin versions for reproducibility)
RUN pip install --no-cache-dir \
    dbt-core==1.10.15 \
    dbt-sqlserver==1.9.0

# Workdir
WORKDIR /app

# Copy dbt project into image (simple & reliable)
COPY . /app/dbt

# Default: run dbt from project dir, using profiles in same folder
WORKDIR /app/dbt
ENV DBT_PROFILES_DIR=/app/dbt

ENTRYPOINT ["dbt"]
