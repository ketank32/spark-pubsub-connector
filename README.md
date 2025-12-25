# Spark Pub/Sub Connector

![Build](https://img.shields.io/badge/build-maven-blue)
![Scala](https://img.shields.io/badge/scala-2.12-red)
![License](https://img.shields.io/badge/license-Apache%202.0-green)

A **production‑ready Apache Spark connector for Google Cloud Pub/Sub** that supports **reading and writing streaming data** using **Spark Structured Streaming**.

This connector implements a custom **Spark DataSource (V1)** that enables Spark applications to consume messages from **Pub/Sub subscriptions** and publish messages to **Pub/Sub topics** in a scalable and fault‑tolerant way.

---

## Features

* Structured Streaming **source** for Google Cloud Pub/Sub
* Structured Streaming **sink** for Pub/Sub topics
* Automatic schema inference
* Offset tracking for exactly‑once semantics (best effort)
* Written in **Scala**, optimized for Spark 3.x

---

## Requirements

* Apache Spark **3.x**
* Java **8+**
* Maven
* Google Cloud Pub/Sub project
* Pub/Sub topic and subscription
* Google Cloud authentication configured (ADC or service account)

---

## Build

```bash
git clone https://github.com/ketank32/spark-pubsub-connector.git
cd spark-pubsub-connector
mvn clean package
```

The JAR will be generated under:

```
target/spark-pubsub-connector-<version>.jar
```

---

## Usage

### Read from Pub/Sub

```scala
val df = spark.readStream
  .format("pubsub")
  .option("projectId", "my-gcp-project")
  .option("subscription", "projects/my-gcp-project/subscriptions/my-sub")
  .load()

val query = df.writeStream
  .format("console")
  .start()

query.awaitTermination()
```

---

### Write to Pub/Sub

```scala
df.writeStream
  .format("pubsub")
  .option("projectId", "my-gcp-project")
  .option("topic", "projects/my-gcp-project/topics/my-topic")
  .option("checkpointLocation", "/tmp/pubsub-checkpoint")
  .start()
```

---

## Configuration Options

| Option Key           | Required     | Description                    |
| -------------------- | ------------ | ------------------------------ |
| `projectId`          | Yes          | Google Cloud project ID        |
| `subscription`       | Yes (source) | Full Pub/Sub subscription path |
| `topic`              | Yes (sink)   | Full Pub/Sub topic path        |
| `checkpointLocation` | Yes (sink)   | Spark checkpoint directory     |

Authentication is handled using **Google Application Default Credentials** or service account credentials provided to the Spark runtime.

---

## Schema

The source produces a fixed schema inferred from `PubSubMessage`:

```text
subscription        STRING
offset              LONG
data                BINARY
publish_timestamp   LONG
event_timestamp     LONG
attributes          STRING
message_id          STRING
```

* `data` contains the raw Pub/Sub payload
* Timestamps are represented as epoch milliseconds

---

## Spark Submit Example

```bash
spark-submit \
  --class your.main.Class \
  --master local[*] \
  --jars target/spark-pubsub-connector-1.0.0.jar \
  your-app.jar
```

---

## Project Structure

```
src/main/scala/io/github/ketank32/spark/pubsub
├── DefaultSource.scala
├── PubSubStreamingSource.scala
├── PubSubStreamingSinkProvider.scala
├── PubSubMessage.scala
├── PubSubUtil.scala
├── PubSubSourceOffset.scala
```

---

## Limitations

* Uses Spark **DataSource V1** (V2 not yet supported)
* Message attributes are serialized as a string
* No built‑in schema registry

---

## Contributing

Contributions, issues, and feature requests are welcome.

---

## License

Apache License 2.0

---

## Author

**Ketan Kumbhar**
