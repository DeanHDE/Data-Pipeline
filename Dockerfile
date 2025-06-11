# For more information, please refer to https://aka.ms/vscode-docker-python
FROM apache/airflow:3.0.1-python3.12

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Ensure the database directory is initialized and persists in a Docker volume
# In your Dockerfile, add these lines to copy and run the script during the image build:
USER root
COPY install-postgres.sh /install-postgres.sh
RUN chmod +x /install-postgres.sh && /install-postgres.sh
USER airflow
# If constraint is off, Airflow installation might fail
# Install pip requirements
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt 

WORKDIR /app
COPY . /app

