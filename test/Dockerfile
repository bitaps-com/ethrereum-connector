FROM ubuntu:20.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install python3 python3-pip build-essential libtool autotools-dev automake pkg-config libssl-dev libevent-dev git
COPY requirements.txt .
RUN pip3 install --requirement requirements.txt
RUN mkdir /ethereumconnector
RUN mkdir /test
WORKDIR /test
ENTRYPOINT ["python3", "testconnector.py"]
