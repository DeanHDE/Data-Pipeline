# For more information, please refer to https://aka.ms/vscode-docker-python
FROM ubuntu:latest

# Install pip, Python, then Java for Spark
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv bash openjdk-17-jdk wget
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"
RUN mkdir -p /opt/bitnami/spark/jars && \
    wget -O /opt/bitnami/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN pip install uv
COPY requirements.txt .
RUN uv pip install -r requirements.txt
RUN uv pip install pre-commit \
    && pre-commit install \
    && pre-commit run --all-files || true


WORKDIR /app
COPY . /app

CMD ["/bin/sh"]

