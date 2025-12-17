# Pin to Debian 12 (bookworm) so Microsoft Debian 12 repo works reliably
FROM python:3.11-slim-bookworm

# System deps + unixODBC headers
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg ca-certificates apt-transport-https \
    unixodbc unixodbc-dev \
  && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server (Debian 12)
RUN set -eux; \
  mkdir -p /etc/apt/keyrings; \
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg; \
  chmod go+r /etc/apt/keyrings/microsoft.gpg; \
  echo "deb [signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list; \
  apt-get update; \
  ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18; \
  rm -rf /var/lib/apt/lists/*

# Install dbt + adapter (pin versions)
RUN pip install --no-cache-dir \
    dbt-core==1.10.15 \
    dbt-sqlserver==1.9.0

WORKDIR /app
COPY . /app/dbt

WORKDIR /app/dbt
ENV DBT_PROFILES_DIR=/app/dbt

ENTRYPOINT ["dbt"]
