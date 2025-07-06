### data science image
FROM ubuntu:20.04

COPY . /root/

### install some packages
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip install uv
RUN uv pip install -p /root/

