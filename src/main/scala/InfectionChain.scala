import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object InfectionChain {
  def main(args: Array[String]): Unit = {

    // Setup Spark configuration
    val conf = new SparkConf().setAppName("infection-chain").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(1))

    streamingContext.sparkContext.setLogLevel("WARN")

    // Configure Kafka
    val sharedKafkaConfig = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092"
    )

    val kafkaConsumerProperties = Map[String, Object](
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> ("infection-chain-" + Random.nextInt.abs.toString),
      "auto.offset.reset" -> "earliest", // Retrieve all messages
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ) ++ sharedKafkaConfig

    val contactsStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array("contacts"), kafkaConsumerProperties)
    )

    /**
     * Hashes userIds to VertexIds
     */
    def nnHash(userId: String): VertexId = userId.hashCode & 0x7FFFFF

    val users = streamingContext.sparkContext.emptyRDD[(VertexId, (String, Unit))]
    val defaultUser = ("Unknown", -1)
    val contacts: EdgeRDD[Long] = EdgeRDD.fromEdges(streamingContext.sparkContext.emptyRDD[Edge[Long]])

    // Track state in this graph variable.
    var graph: Graph[(String, Unit), Long] = Graph(users, contacts, defaultUser)

    // Maintain the graph
    contactsStream
      .map(m => m.value().split(",") -> m.timestamp()) // split the input string by commas
      .foreachRDD { rdd =>
        // Get the newly added users
        val users = rdd
          .flatMap(m => Array(m._1(0), m._1(1))) // Get the first two keys
          .distinct
        val newVertices = users
          .keyBy(nnHash)
          // Filter out all already present users - not necessary when the vertex attribute is constant
          // .subtract(graph.vertices.mapValues(_._1))
          .mapValues(_ -> Unit)

        // Combine new vertices with the old ones
        val vertices = graph.vertices.union(newVertices).distinct

        val newEdges: RDD[Edge[Long]] = rdd.map(v => nnHash(v._1(0)) -> nnHash(v._1(1)) -> v._2)
          .map { case ((id1, id2), ts) => new Edge(id1, id2, ts) }

        val edges = graph.edges.union(newEdges).distinct

        // Update our graph
        graph = Graph(vertices, edges, defaultUser)

        println("-------------------")
        println(vertices.count() + " " + edges.count())
        graph.triplets.collect().foreach { v => println(v.srcAttr._1 + "\t=>\t" + v.dstAttr._1 + "\t" + (v.attr % 1586510000000L)) }
      }

    val infectionsStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array("infections"), kafkaConsumerProperties)
    )

    // Kafka producer configuration
    val kafkaProducerProperties = new Properties()
    sharedKafkaConfig.foreach { case (key, value) => kafkaProducerProperties.put(key, value) }
    kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    var infectedUsers = streamingContext.sparkContext.emptyRDD[(VertexId, (String, Long))]

    infectionsStream
      .map(m => m.value() -> m.timestamp()) // Get only the value
      .map { case (userId, timestamp) => (nnHash(userId) -> (userId, timestamp)) } // Key by the hash
      .foreachRDD { rdd =>
        // Add new infected users to the infectedUsers RDD
        infectedUsers = infectedUsers.fullOuterJoin(rdd)
          .mapValues { case (left, right) => left.getOrElse("" -> Long.MaxValue) -> right.getOrElse("" -> Long.MaxValue)}
          .mapValues { case ((leftUserId, leftTs), (rightUserId, rightTs)) =>
            (( if (leftUserId != "") leftUserId else rightUserId) -> leftTs.min(rightTs))
          }

        // Create a graph with infectedUsers
        val infectedGraphVertices = graph.vertices
          .leftJoin(infectedUsers){ case (vertexId: VertexId, (userId: String, _: Unit), option) =>
            if (option.isDefined) {
              vertexId -> (userId, Map[Int, Long](0 -> option.get._2))
            } else {
              vertexId -> (userId, Map[Int, Long]())
            }
          }

        val infectedGraph: Graph[(String, Map[Int, Long]), Long] = Graph(infectedGraphVertices, contacts)

        infectedGraph.pregel[(Int, Long)]((-1, -1), 3)(
          (_: VertexId, vertexData: (String, Map[Int, Long]), message: (Int, Long)) => {
            if (message == (-1, -1)) {
              (vertexData._1, vertexData._2)
            } else {
              (vertexData._1, vertexData._2.+(message))
            }
          },
          triplet => {
            val dstMap = triplet.dstAttr._2
            val srcMap = triplet.srcAttr._2
            val contactTime = triplet.attr

            srcMap
              // Only include infections prior to this contact of happening
              .filter { case (level, ts) => ts < contactTime}
              // Only include infections which would mark them as at risk at an earlier time
              .filter { case (level, ts) => dstMap.getOrElse(level, Long.MaxValue) > contactTime }
              .map { case (level, ts) => triplet.dstId -> (level, contactTime)}
              .toIterator
          },
          (m1: (Int, Long), m2: (Int, Long)) => {
            // Prefer the message with the lowest level
            if (m1._1 < m2._1) {
              m1
            } else if (m2._1 < m1._1) {
              m2
            } else { // Else take the oldest message
              m1._1 -> m1._2.min(m2._2)
            }
          }
        )
      }

    // Start the computation
    streamingContext.start()
    streamingContext.awaitTermination()

    println("Finished")
  }

}
