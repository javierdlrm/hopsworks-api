FROM locustio/locust:2.23.1

USER root

RUN apt-get update -y && apt-get -y install gcc python3-dev librdkafka-dev git

USER locust
WORKDIR /home/locust

COPY requirements.txt .
RUN pip3 install -r requirements.txt
