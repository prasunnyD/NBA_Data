FROM arm64v8/ubuntu:latest


RUN apt-get update && apt-get install -y \
    python3.9 \
    python3-pip

COPY . /

RUN pip3 install -r requirements.txt