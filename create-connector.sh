# Create the connector using the configuration file
curl -X POST -H "Content-Type: application/json" --data @src/connectors/postgres-source.json http://localhost:8083/connectors
curl -X POST -H "Content-Type: application/json" --data @src/connectors/clickhouse-sink.json http://localhost:8083/connectors


