# Spark Streaming Location Tracker

**TL;DR**: This service listens on a Kafka topic for events containing locations, computes which of these locations are close to each other, and publishes to a different topic all locations which are close to each other.

## Usage

First, make sure that there's a Kafka instance running locally, this can be achieved by running:

```bash
docker-compose up
```

Then, start the `src/main/scala/Main` class. This project uses Java 1.8, newer versions will not be compatible with the used Spark version.

To play around with it, open two shells, one running:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic result
```

And another one running:

```
kafka-console-producer --topic topicA --broker-list localhost:9092
```

At this point you can send some demo data through the producer shell, e.g. this snippet (submit with shift-enter as to prevent also sending an empty message):

```csv
-88.331492,32.324142,hotel
-88.175933,32.360763,gas
-88.388954,32.357073,bar
-88.221102,32.35078,restaurant
```

Now you will see in your consumer shell that several new messages appeared:

```csv
hotel,bar,0.066232283462637
gas,restaurant,0.0462561402323107
bar,hotel,0.066232283462637
restaurant,gas,0.0462561402323107
```

In some cases, less events will be outputted, as it might be that your input was processed in two seperate batches. Currently, this is likely to happen, as the batch size is set to 1 second. Future versions with larger batch sizes should make this less significant.