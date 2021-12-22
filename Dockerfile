FROM openjdk:latest
ARG Pathdir="/consumers"
RUN mkdir $Pathdir
COPY target/kafka-consumer-metrics-0.1.0-jar-with-dependencies.jar $Pathdir
WORKDIR $Pathdir
ADD topics-list.json .
ENTRYPOINT ["java", "-jar", "kafka-consumer-metrics-0.1.0-jar-with-dependencies.jar", "bitnami-kafka-0.bitnami-kafka-headless.monitoring.svc.cluster.local:9092", "topics-list.json"]