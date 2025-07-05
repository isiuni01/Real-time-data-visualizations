# Real-time Streaming Analytics Platform

A comprehensive real-time data streaming and analytics platform built with Apache Flink, Apache Kafka, and a complete monitoring stack. This project provides a scalable solution for processing boat navigation and racing data in real-time.

## 🏗️ Architecture

The platform consists of the following components:

### Core Streaming Components
- **Apache Kafka** - Message broker for real-time data streaming
- **Apache Flink** - Stream processing engine for real-time analytics
- **Zookeeper** - Coordination service for Kafka

### Monitoring & Visualization
- **Prometheus** - Metrics collection and monitoring
- **Grafana** - Data visualization and dashboards
- **InfluxDB** - Time-series database for boat telemetry data
- **Telegraf** - Data collection agent for Kafka-to-InfluxDB pipeline
- **Kafdrop** - Kafka cluster management UI

## 📊 Data Processing

The platform processes two main data streams:

1. **Navigation Data** (`boat_data_navigation` topic)
   - Real-time boat position, speed, and navigation metrics
   - Processed by Telegraf and stored in InfluxDB

2. **Race Data** (`boat_race_data` topic)
   - Boat-to-boat racing analytics and competition metrics
   - Tagged with race numbers and opponent information

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 4GB of available RAM

### Build and Run

1. **Build the custom Flink image:**
   ```bash
   docker build -t myflink .
   ```

2. **Start the entire stack:**
   ```bash
   docker-compose up -d
   ```

3. **Verify all services are running:**
   ```bash
   docker-compose ps
   ```

## 🔧 Service Configuration

### Port Mapping
- **Kafka**: `9091` (external), `19091` (internal)
- **Zookeeper**: `2181`
- **Kafdrop**: `9000` (Kafka UI)
- **Flink JobManager**: `8081` (Web UI)
- **Grafana**: `3000` (admin/admin2025)
- **InfluxDB**: `8086` (admin/admin2025)
- **Prometheus**: `9090`

### Metrics Endpoints
- **Kafka JMX**: `9308`
- **Flink JobManager**: `9249`
- **Flink TaskManager**: `9250`
- **Telegraf**: `9273`

## 📈 Monitoring

### Prometheus Metrics Collection
The platform automatically collects metrics from:
- Kafka broker (JMX metrics via custom exporter)
- Flink JobManager and TaskManager
- InfluxDB
- Telegraf

### Grafana Dashboards
Access Grafana at `http://localhost:3000` with:
- Username: `admin`  
- Password: `admin2025`

Pre-configured with:
- Kafka datasource plugin for direct topic monitoring
- InfluxDB datasource for boat telemetry visualization

### InfluxDB Data Storage
- **Organization**: `my-org`
- **Bucket**: `Navigation` (for boat data)
- **Token**: Auto-generated admin token

## 🛠️ Development

### Flink Job Deployment

1. **Access Flink SQL Client:**
   ```bash
   docker-compose exec sql-client bin/sql-client.sh
   ```

2. **Submit jobs via Flink Web UI:**
   Navigate to `http://localhost:8081`

### Custom Flink Connectors
The platform includes pre-built connectors:
- `flink-connector-kafka-4.0.0-2.0.jar`
- `flink-json-2.0.0.jar`
- `kafka-clients-3.7.0.jar`
- `flink-metrics-prometheus-2.0.0.jar`

### Configuration Files

- **Flink Configuration**: `flink-conf.yaml`
  - Prometheus metrics enabled
  - Memory settings optimized for containerized deployment
  - Failover strategy configured

- **Kafka JMX Configuration**: `kafka-jmx-config.yml`
  - Custom metric patterns for Prometheus export
  - Topic and partition-level metrics

- **Telegraf Configuration**: `telegraf.conf`
  - Kafka consumer configuration for boat data topics
  - InfluxDB output with proper tagging
  - Prometheus metrics endpoint

## 🔍 Troubleshooting

### Common Issues

1. **Out of Memory Errors**
   - Increase Docker memory allocation to at least 4GB
   - Adjust Flink memory settings in `flink-conf.yaml`

2. **Kafka Connection Issues**
   - Ensure all services are fully started (use `docker-compose logs`)
   - Check if Zookeeper is healthy before Kafka starts

3. **Metrics Not Appearing**
   - Verify JMX agents are properly loaded
   - Check Prometheus targets at `http://localhost:9090/targets`

### Logs Access
```bash
# View all services
docker-compose logs

# Specific service logs
docker-compose logs kafka
docker-compose logs flink-jobmanager
docker-compose logs telegraf
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with the full stack
5. Submit a pull request

## 📝 License

This project is part of a Large-Scale Systems course implementation focusing on real-time stream processing architectures.

---

**Note**: This setup is optimized for development and educational purposes. For production deployment, consider additional security configurations, resource limits, and high-availability setups.
