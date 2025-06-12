# For more information, please refer to https://aka.ms/vscode-docker-python
FROM ubuntu:latest

# Install pip requirements
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv bash
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt


WORKDIR /app
COPY . /app

CMD ["/bin/sh"]

