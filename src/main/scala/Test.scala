import java.util.Properties

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}

object Test {
  def main(args: Array[String]): Unit = {

    // Setup Spark configuration
    val conf = new SparkConf().setAppName("whatever").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(1))

    streamingContext.sparkContext.setLogLevel("WARN")

    // Configure Kafka
    val sharedKafkaConfig = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092"
    )

    val kafkaConsumerProperties = Map[String, Object](
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "testid",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ) ++ sharedKafkaConfig

    val topics = Array("topicA")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaConsumerProperties)
    )

    val geoFactory: GeometryFactory = new GeometryFactory()

    // Kafka producer configuration
    val kafkaProducerProperties = new Properties()
    sharedKafkaConfig.foreach { case (key, value) => kafkaProducerProperties.put(key, value)}
    kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    stream.foreachRDD { rdd =>
      val points: RDD[Point] = rdd.map(_.value().split(",")) // split the input string by commas
        .map(v => v(0).toFloat -> v(1).toFloat -> v(2))
        .map { case ((x, y), id) => new Coordinate(x, y) -> id }
        .map { case (coordinate, id) => geoFactory.createPoint(coordinate) -> id }
        .map { case (point, id) => point.setUserData(id); point }

      if (points.count() > 1) {
        val pointRDD = new PointRDD(points)

        val joinedPoints = geoSpatialJoin(pointRDD)

        joinedPoints
          .map { case (id1, id2, distance) => new ProducerRecord[String, String]("result", null, "%s,%s,%s".format(id1, id2, distance))}
          .foreachPartition { records =>
            val producer = new KafkaProducer[String, String](kafkaProducerProperties);

            records.foreach { producer.send }
          }
      }
    }

    // Start the computation
    streamingContext.start()
    streamingContext.awaitTermination()

    println("Finished")
  }

  def geoSpatialJoin(pointRDD: PointRDD): RDD[(AnyRef, AnyRef, Double)] = {
    pointRDD.analyze()

    val circleRDD = new CircleRDD(pointRDD, 0.1)  // Set the boundary to 0.1 radian
    circleRDD.analyze()
    circleRDD.spatialPartitioning(GridType.KDBTREE)

    pointRDD.spatialPartitioning(circleRDD.getPartitioner)

    // Execute the distance join
    val considerBoundaryIntersection = false
    val usingIndex = false

    val resultAsJavaPairRDD = JoinQuery.DistanceJoinQueryFlat(pointRDD, circleRDD, usingIndex, considerBoundaryIntersection)

    // Convert the value to a Scala native version
    val result = JavaPairRDD.toRDD(resultAsJavaPairRDD)

    // Filter out self-matches and compute the distance between points
    result
      .filter { case (from, to) => from.getUserData != to.getUserData } // Filter out all self-matches
      .map{ case (from, to) => (from.getUserData, to.getUserData, from.distance(to))} // Compute the distance between all points
  }
}
