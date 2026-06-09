# Smart Building HVAC Monitoring — Spark Structured Streaming

A real-time IoT data processing pipeline built with Apache Spark 
Structured Streaming, simulating a smart building HVAC monitoring system.

## What it does
- Ingests simulated sensor data (temperature, humidity) at 5 records/second
- Runs 3 concurrent real-time SQL queries on the stream:
  - Detects rooms with critical temperatures (< 18°C or > 60°C)
  - Calculates average readings over 1-minute sliding windows
  - Flags rooms needing attention based on humidity thresholds
- Outputs all streams to console in real time

## Tech used
- Apache Spark Structured Streaming
- PySpark SQL
- Windowed aggregations
- Multi-stream processing

## What I'd add in production
- Write streams to a data warehouse instead of console
- Add alerting (email/Slack) when critical thresholds are breached
- Containerise with Docker for deployment
- Add schema validation on inbound sensor data
