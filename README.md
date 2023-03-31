# Kafka Consumer Metrics

[![GitHub license](https://img.shields.io/github/license/smartx-usman/kafka-consumer-metrics?logoColor=lightgrey&style=plastic)](https://github.com/OFTEIN-NET/OFTEIN-MultiTenantPortal/blob/main/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/smartx-usman/kafka-consumer-metrics?style=plastic)](https://github.com/OFTEIN-NET/OFTEIN-MultiTenantPortal/issues)
[![GitHub forks](https://img.shields.io/github/forks/smartx-usman/kafka-consumer-metrics?style=plastic)](https://github.com/OFTEIN-NET/OFTEIN-MultiTenantPortal/network)
[![Actions Status](https://github.com/smartx-usman/kafka-consumer-metrics/workflows/Build%20Kafka%20Consumer%20for%20Telegraf/badge.svg)](https://github.com/OFTEIN-NET/OFTEIN-MultiTenantPortal/actions)

A set of Kafka consumer tools developed to parse Telegraf metrics and store to File/Elasticsearch/Prometheus.

### Prerequisites
- Kubernetes 1.12+
- Helm 3.1.0+

### Tool Versions Used
| Tool | Helm Version | Tool Version |
| --- | --- | --- |
| Apache ZooKeeper | 7.5.1 | 2.8.0 | 
| Apache kafka | 14.0.0 | 2.8.0 | 
| Telegraf | - | 1.20.4 | 
| Elasticsearch | 7.16.2 | 7.16.2 | 

### How to Build JAR from Source
This is a Maven project. So, Maven is used to generate JAR with all the dependencies.

### How to Build Docker Image on macOS (M1 chip)
```bash
docker buildx build --platform linux/amd64,linux/arm64 --push -t usman476/kafka-consumer-metrics:latest -t usman476/kafka-consumer-metrics:0.2.0 .
```

### How to Deploy on Kubernetes
```bash
kubectl apply -f https://github.com/smartx-usman/Distributed-Observability-Framework/blob/main/tools/kafka-consumers/kc-nm-deployment.yaml
```