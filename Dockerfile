FROM debian:buster
MAINTAINER yohan <783b8c87@scimetis.net>
ENV DEBIAN_FRONTEND noninteractive
ENV TZ Europe/Paris
RUN apt-get update && apt-get -y install python3 python3-requests python3-yaml
WORKDIR /root
COPY script.py /root/
ENTRYPOINT ["/root/script.py"]
