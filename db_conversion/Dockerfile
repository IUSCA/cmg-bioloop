FROM python:3.10

WORKDIR /opt/sca/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the script and requirements

COPY requirements.txt .
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy env variables file
COPY .env .

# Copy source code
RUN mkdir src
COPY src src/
