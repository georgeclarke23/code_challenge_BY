FROM ubuntu:latest

# Install OpenJDK 8
RUN \
  apt-get update && \
  apt-get install -y openjdk-8-jdk && \
  rm -rf /var/lib/apt/lists/*

# Install Python
RUN \
    apt-get update && \
    apt-get install -y python3 python3-dev python3-pip python3-virtualenv zip && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/src
COPY requirements.txt Makefile /opt/

RUN pip3 install --upgrade pip
RUN pip3 install -r /opt/requirements.txt && pip freeze

COPY src /opt/src

# Define working directory
WORKDIR /opt

ENTRYPOINT ["make","run/spark"]


