FROM openjdk:slim-buster

ARG PATH_DIR="/consumers"
ARG TOPICS_LIST="topics-list.json"

RUN mkdir $PATH_DIR

COPY target/kafka-consumer-metrics-0.1.0-jar-with-dependencies.jar $PATH_DIR

WORKDIR $PATH_DIR

ADD $TOPICS_LIST .

#RUN echo "#!/bin/bash" > ./entrypoint.sh
#RUN echo "java -jar ${JAR_FILE} ${KAFKA_BROKER} ${TOPICS_LIST} ${DATA_STORE} ${ES_HOSTNAME} ${ES_PORT} ${ES_INDEX_RETENTION_DAYS} ${PROMETHEUS_PUSHGATEWAY}" >> ./entrypoint.sh
#RUN chmod +x ./entrypoint.sh

#ENTRYPOINT ["./entrypoint.sh"]