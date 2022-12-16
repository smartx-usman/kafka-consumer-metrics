FROM openjdk:slim-buster

ARG PATH_DIR="/consumers"
ARG JAR_FILE="kafka-consumer-metrics-0.1.0-jar-with-dependencies.jar"
ARG KAFKA_BROKER="bitnami-kafka-0.bitnami-kafka-headless.observability.svc.cluster.local:9092"
ARG ES_HOSTNAME="es-master-headless.observability.svc.cluster.local"
ARG ES_PORT=9200
ARG ES_INDEX_RETENTION_DAYS="3d"
ARG PROMETHEUS_PUSHGATEWAY="prometheus-prometheus-pushgateway.observability.svc.cluster.local:9091"


ARG TOPICS_LIST="topics-list.json"

RUN mkdir $PATH_DIR

COPY target/kafka-consumer-metrics-0.1.0-jar-with-dependencies.jar $PATH_DIR

WORKDIR $PATH_DIR

ADD $TOPICS_LIST .

RUN echo "#!/bin/bash" > ./entrypoint.sh
RUN echo "java -jar ${JAR_FILE} ${KAFKA_BROKER} ${TOPICS_LIST} ${ES_HOSTNAME} ${ES_PORT} ${ES_INDEX_RETENTION_DAYS} ${PROMETHEUS_PUSHGATEWAY}" >> ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]