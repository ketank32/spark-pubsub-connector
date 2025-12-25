# Spark Pub/Sub Connector

A **simple Apache Spark connector for Google Cloud Pub/Sub** that supports **reading and writing streaming data** using **Spark Structured Streaming**.

This project provides a custom Spark DataSource implementation that allows Spark jobs to consume messages from Pub/Sub subscriptions and publish messages to Pub/Sub topics.

---

## Features

* Read data from **Google Cloud Pub/Sub** using Spark Structured Streaming
* Write streaming data from Spark to **Pub/Sub topics**
* Works with **Spark DataSource V1**
* Implemented in **Scala**

---

## Requirements

* Apache Spark 3.x
* Java 8+
* Maven
* Google Cloud Pub/Sub project, topic, and subscription
* Google Cloud credentials configured

---

## Build

Clone the repository and build using Maven:

```bash
git clone https://github.com/ketank32/spark-pubsub-connector.git
cd spark-pubsub-connector
mvn clean package
```

The build will generate a JAR under the `target/` directory.

---

## Usage

### Read from Pub/Sub (Structured Streaming)

```scala
val df = spark.readStream
  .format("pubsub")
  .option("projectId", "your-gcp-project")
  .option("subscription", "projects/PROJECT_ID/subscriptions/SUBSCRIPTION_NAME")
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
  .option("projectId", "your-gcp-project")
  .option("topic", "projects/PROJECT_ID/topics/TOPIC_NAME")
  .option("checkpointLocation", "/tmp/pubsub-checkpoint")
  .start()
```

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
├── PubSubStreamingSink.scala
├── PubSubUtil.scala
├── PubSubMessage.scala
```

Test examples are available under:

```
src/main/scala/test
```

---

## Limitations

* DataSource V1 implementation
* Limited configuration options (can be extended)
* Basic message schema

---

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

---

## License

Apache License 2.0

---

## Author

**Ketan Kumbhar**
