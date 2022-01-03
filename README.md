# Kafka Consumer Metrics

[![GitHub license](https://img.shields.io/github/license/smartx-usman/kafka-consumer-metrics?logoColor=lightgrey&style=plastic)](https://github.com/OFTEIN-NET/OFTEIN-MultiTenantPortal/blob/main/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/smartx-usman/kafka-consumer-metrics?style=plastic)](https://github.com/OFTEIN-NET/OFTEIN-MultiTenantPortal/issues)
[![GitHub forks](https://img.shields.io/github/forks/smartx-usman/kafka-consumer-metrics?style=plastic)](https://github.com/OFTEIN-NET/OFTEIN-MultiTenantPortal/network)
[![Actions Status](https://github.com/smartx-usman/kafka-consumer-metrics/workflows/Build%20Kafka%20Consumer%20for%20Telegraf/badge.svg)](https://github.com/OFTEIN-NET/OFTEIN-MultiTenantPortal/actions)

A set of Kafka consumer tools developed to parse Telegraf metrics and store to Elasticsearch for AIDA project.

### Prerequisites
- Kubernetes 1.12+
- Helm 3.1.0+

### Tools Versions
| Tool | Helm Version | Tool Version |
| --- | --- | --- |
| Apache ZooKeeper | 7.5.1 | 2.8.0 | 
| Apache kafka | 14.0.0 | 2.8.0 | 
| Telegraf | - | 1.20.4 | 
| Elasticsearch | 7.16.2 | 7.16.2 | 

### How to Build
This is a Maven project. So, Maven is used to generate JAR with all the dependencies.

### How to Run
kubectl apply -f [kc-nm-pod.yaml](https://github.com/smartx-usman/IIoT-Edge-Observability/blob/main/tools/kafka-consumers/kc-nm-pod.yaml)