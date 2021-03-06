FROM openjdk:slim-buster

ARG PATH_DIR="/consumers"
ARG JAR_FILE="kafka-consumer-metrics-0.1.0-jar-with-dependencies.jar"
ARG KAFKA_BROKER="bitnami-kafka-0.bitnami-kafka-headless.observability.svc.cluster.local:9092"
ARG TOPICS_LIST="topics-list.json"

RUN mkdir $PATH_DIR

COPY target/kafka-consumer-metrics-0.1.0-jar-with-dependencies.jar $PATH_DIR

WORKDIR $PATH_DIR

ADD $TOPICS_LIST .

RUN echo "#!/bin/bash" > ./entrypoint.sh
RUN echo "java -jar ${JAR_FILE} ${KAFKA_BROKER} ${TOPICS_LIST}" >> ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]