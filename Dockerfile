# For more information, please refer to https://aka.ms/vscode-docker-python
FROM apache/airflow:3.0.1-python3.12

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# If constraint is off, Airflow installation might fail
# Install pip requirements
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt 

WORKDIR /app
COPY . /app

