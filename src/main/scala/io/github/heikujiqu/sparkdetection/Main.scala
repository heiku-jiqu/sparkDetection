package io.github.heikujiqu.sparkDetection

import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.HashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast

object Main {
  def main(args: Array[String]): Unit = {
    // parse args manually, no need add another dependency
    var detectionFilePath = "/testdata/detection.parquet"
    var locationFilePath = "/testdata/location.parquet"
    var outputFilePath = "/testdata/output.parquet"
    var topN = 10
    args.sliding(2, 2).toList.collect {
      case Array("--detectionFilePath", argPath: String) =>
        detectionFilePath = argPath
      case Array("--locationFilePath", argPath: String) =>
        locationFilePath = argPath
      case Array("--outputPath", argPath: String) => locationFilePath = argPath
      case Array("--topN", argTopN: String)       => topN = argTopN.toInt
    }

    val kryoConf = new SparkConf()
    kryoConf
      .registerKryoClasses(
        Array(
          classOf[Location],
          classOf[Detection],
          classOf[AggDetection],
          classOf[Output]
        )
      )
      .set(
        "spark.serializer",
        "org.apache.spark.serializer.KryoSerializer"
      ) // faster Serializer than JavaSerializer

    val spark = SparkSession
      .builder()
      .appName("Top 10 Video Detection by Geographical location")
      .config(
        "spark.eventLog.enabled",
        "true"
      ) // turn on eventLog for history server
      .config(
        "spark.eventLog.dir",
        "/opt/spark/spark-events"
      ) // save eventLog for history server
      .config(kryoConf)
      .config(
        "spark.dynamicAllocation.enabled",
        true
      ) // scales the number of executors registered with this application up and down based on the workload
      .getOrCreate()

    import spark.implicits._ // for type conversion
    // when reading detectionRDD, can also do projection pushdown using
    // .select("geographical_location_oid", "detection_oid", "item_name")
    // since we only need these 3 columns
    val detectionRDD = spark.read.parquet(detectionFilePath).as[Detection].rdd
    val locationRDD = spark.read.parquet(locationFilePath).as[Location].rdd

    val outputRDD =
      VideoDetectionTransformations.run(spark, detectionRDD, locationRDD, topN)

    outputRDD.toDF().write.mode("overwrite").parquet(outputFilePath)
  }
}

case class Detection(
    geographical_location_oid: BigInt,
    video_camera_oid: BigInt,
    detection_oid: BigInt,
    item_name: String,
    timestamp_detected: BigInt
)

case class AggDetection(
    geographical_location_oid: BigInt,
    item_name: String,
    count: BigInt,
    rank: Int
)

case class Location(
    geographical_location_oid: BigInt,
    geographical_location: String
)

case class Output(
    geographical_location: String,
    item_rank: Int,
    item_name: String
)

object VideoDetectionTransformations {
  private val TOPN_DEFAULT = 10

  /** Main entrypoint to transform input RDDs into output RDD
  *
  *
  * @param spark
  * @param detectionRDD Dataset A
  * @param locationRDD Dataset B
  * @param topN Number of highest count item_names per geographical_location to return
  * @return
  */
  def run(
      spark: SparkSession,
      detectionRDD: RDD[Detection],
      locationRDD: RDD[Location],
      topN: Int = TOPN_DEFAULT
  ): RDD[Output] = {
    // Broadcast join since RDD[Location] is small and fixed size
    val iter = locationRDD.toLocalIterator.map { loc =>
      (loc.geographical_location_oid, loc.geographical_location)
    }.toSeq
    val locationHashMap: HashMap[BigInt, String] = HashMap(iter: _*)
    val locationHashMapBroadcast = spark.sparkContext.broadcast(locationHashMap)

    detectionRDD
      .map(x =>
        ((x.geographical_location_oid, x.item_name, x.detection_oid), 1)
      )
      .repartitionAndSortWithinPartitions(new CustomGeogLocPartitioner(12))
      .mapPartitions(topNInPartition(_, locationHashMapBroadcast))
      .flatMap(a => a)

      // To handle skewed partitions, first repartition with GeogLocSkewPartitioner
      // do the same top N in each partition
      // then repartition again based on just geographical_location_oid
      // then combine the results by taking top 10 of each geographical_location_oid.
      // Probably can use .fold to combine HashMaps.
      //
      // detectionRDD
      // .map( x=>
      //   ((x.geographical_location_oid, x.item_name, x.detection_oid), 1)
      // )
      // .repartitionAndSortWithinPartitions(new GeogLocSkewPartitioner(12))
      // .mapPartitions(topNInPartition(_, locationHashMapBroadcast))
      // .flatMap(a => a.map(output => (output.geographical_location, output)))
      // .repartitionAndSortWithinPartitions(new CustomGeogLocPartitioner(12))
      // .mapPartitions( x => {
      //   x.toSeq.groupBy(_._1).valuesIterator.map( x => x.take(10).map(_._2)).flatten
      // })
  }

  /** Returns count and name of top 10 items in each geographical location
    * Assumes partition is sorted
    */
  private def topNInPartition(
      partition: Iterator[((BigInt, String, BigInt), Int)],
      locationHashMapBroadcast: Broadcast[HashMap[BigInt, String]],
      topN: Int = TOPN_DEFAULT
  ): Iterator[Array[Output]] = {
    val hm = HashMap[BigInt, HashMap[String, Int]]()

    // count each row into the HashMap
    partition.sliding(2).foreach {
      // when detection_id not duplicated, increment the count
      case List(
            ((geog_id, item_name, detect_id), count),
            ((geog_id2, item_name2, detect_id2), count2)
          ) if detect_id != detect_id2 => {
        if (!hm.contains(geog_id)) {
          hm += (geog_id -> HashMap[String, Int]())
        }
        if (!hm(geog_id).contains(item_name)) {
          hm(geog_id) += (item_name -> 0)
        }
        hm(geog_id)(item_name) += count
      }
      // account for odd number of records
      case List(((geog_id, item_name, detect_id), count)) => {
        if (!hm.contains(geog_id)) {
          hm += (geog_id -> HashMap[String, Int]())
        }
        if (!hm(geog_id).contains(item_name)) {
          hm(geog_id) += (item_name -> 0)
        }
        hm(geog_id)(item_name) += count
      }
      // account for duplicates, ie throw away, but still need to match
      case duplicated => println(s"Record is duplicated: $duplicated")
    }

    hm.toIterator.map({
      case (geog_id, v) => {
        v.toArray
          .sortBy(_._2)(Ordering[Int].reverse)
          .take(topN)
          .zipWithIndex
          .map { case ((item_name, count), index) =>
            Output(
              locationHashMapBroadcast.value
                .getOrElse(geog_id, "Location Not Found"),
              index + 1,
              item_name
            )
          }
      }
    })
  }

/** Partition by the first field of key. Only supports Tuple2 and Tuple3.
  */
class CustomGeogLocPartitioner(totalPartitions: Int) extends Partitioner {
  def numPartitions: Int = totalPartitions
  def getPartition(key: Any): Int = key match {
    case (a: BigInt, _) =>
      val rawMod = a.hashCode % numPartitions
      rawMod + (if (rawMod < 0) numPartitions else 0)
    case (a: BigInt, _, _) =>
      val rawMod = a.hashCode % numPartitions
      rawMod + (if (rawMod < 0) numPartitions else 0)
    case a: String =>
      val rawMod = a.hashCode % numPartitions
      rawMod + (if (rawMod < 0) numPartitions else 0)
  }
}

/** Increases the number of partitions for keys that match
  * `skewedGeogLocationOid` by allocating this key with `skewedPartitions`
  * number of partitions out of `totalPartitions` number of partitions.
  */
class GeogLocSkewPartitioner(
    totalPartitions: Int,
    skewedPartitions: Int = 2,
    skewedGeogLocationOid: BigInt = 1
) extends Partitioner {
  require(totalPartitions > 0)
  require(skewedPartitions > 0)
  require(
    totalPartitions - skewedPartitions > 0,
    s"Number of total partitions ($totalPartitions) should be more than number of skewed partitions ($skewedPartitions)"
  )

  def numPartitions: Int = totalPartitions
  private val numPartitionForOthers = numPartitions - 2
  def getPartition(key: Any): Int = {
    key match {
      case geographical_location_oid: BigInt =>
        if (geographical_location_oid == skewedGeogLocationOid) {
          val rawMod = geographical_location_oid.hashCode % skewedPartitions
          val increment = if (rawMod < 0) numPartitions else 0
          rawMod + increment
        } else {
          val rawMod =
            geographical_location_oid.hashCode % numPartitionForOthers
          val increment = if (rawMod < 0) numPartitionForOthers else 0
          skewedPartitions + rawMod + increment
        }
    }
  }
}

  /** Old transformation that involves some suffles
    */
  def oldrun(
      spark: SparkSession,
      detectionRDD: RDD[Detection],
      locationRDD: RDD[Location],
      topN: Int
  ): RDD[Output] = {
    val aggregatedRDD = aggregate(deduplicate(detectionRDD), topN)
    val outputRDD = join(aggregatedRDD, locationRDD)
    outputRDD
  }

  /** Removes duplicated detections. Assumes that the entire row is duplicated.
    * Does not handle when only detection_oid is duplicated while the other
    * fields are different.
    */
  def deduplicate(x: RDD[Detection]): RDD[Detection] = {
    x.distinct()
  }

  /** Count the number of detections, grouped by geographical_location_oid,
    * item_name. Only returns up to the top 10 counts of each group.
    */
  def aggregate(
      x: RDD[Detection],
      topN: Int = TOPN_DEFAULT
  ): RDD[AggDetection] = {
    val rdd = x
      .map(d => ((d.geographical_location_oid, d.item_name), 1))
      .reduceByKey(new CustomGeogLocPartitioner(6), (a, b) => a + b)
      .map({ case ((geographical_location_oid, item_name), count) =>
        (geographical_location_oid, (item_name, count))
      })
      // top 10 and add rank
      .groupByKey()
      .flatMapValues(d => {
        d.toSeq
          .sortBy(_._2)(Ordering[Int].reverse)
          .take(topN)
          .zipWithIndex
          .map { case ((item_name, count), idx) =>
            (item_name, count, idx + 1)
          }
      })
      .map({ case (geographical_location_oid, (item_name, count, rank)) =>
        AggDetection(geographical_location_oid, item_name, count, rank)
      })
    rdd

  }

  /** Aggregation and topN for skewed dataset in one of the geographical
    * locations. Uses custom partitioner GeogLocSkewPartitioner to further
    * partition the skewed location. Uses aggregateByKey to enable partial
    * aggregations when finding topN. Uses minheap in aggregateByKey when
    * finding topN to minimise data needed to shuffle.
    *
    * Below is considerations for the implementation: if big data skew, need
    * decrease the size of partitioning for the skewed group if not a lot of
    * data will concentrate on a node, won't make full use of cluster. The main
    * idea is to partition the single skewed group partition into multiple
    * smaller partitions. First, can try to increase number of partitions at the
    * distinct() stage. Default HashPartitioning won't incur data skew at this
    * point because distinct is using the entire row's hash. Since reduceByKey
    * reduces locally within each partition first without the need for
    * shuffling, having more partitions helps to reduce at partition level
    * before doing final reduce. Second, after outputting (geog_id, item_name,
    * count), we will need to do top 10 items in each geog_id group, if our
    * implementation requires all items within single geog_id to be in memory,
    * we may face data skew issues if there are massive ammount of item_names
    * within a particular skewed geogid. In this case, we will need to partition
    * different items within same geog_id across multiple partitions, then do
    * aggregateByKey where it will get top 10 of each local partition first
    * (with min heap), then combine the heaps across partitions to get overall
    * top 10 for the geog_id. Third, if we know we are going to partition for
    * the top 10 transformation, we can consider partitioning upfront either
    * before the deduplication step, or even ask the datasource to partition the
    * parquet files accordingly. This will help to reduce shuffling.
    */
  def aggregateSkewed(
      x: RDD[Detection],
      topN: Int = TOPN_DEFAULT
  ): RDD[AggDetection] = {
    implicit val minHeapOrdering: Ordering[(String, Int)] =
      Ordering.by[(String, Int), Int](_._2).reverse
    def seqOp(
        pq: PriorityQueue[(String, Int)],
        name_count: (String, Int)
    ): PriorityQueue[(String, Int)] = {
      pq.enqueue(name_count)
      if (pq.size > topN) {
        pq.dequeue()
      }
      pq
    }
    def combOp(
        pq1: PriorityQueue[(String, Int)],
        pq2: PriorityQueue[(String, Int)]
    ): PriorityQueue[(String, Int)] = {
      pq2.foreach { name_count =>
        pq1.enqueue(name_count)
        if (pq1.size > topN) {
          pq1.dequeue()
        }
      }
      pq1
    }

    x.map(d => ((d.geographical_location_oid, d.item_name), 1))
      .reduceByKey(new GeogLocSkewPartitioner(6), (a, b) => a + b)
      .map({ case ((geographical_location_oid, item_name), count) =>
        (geographical_location_oid, (item_name, count))
      })
      // top 10 and add rank
      .aggregateByKey(PriorityQueue.empty[(String, Int)])(seqOp, combOp)
      .flatMapValues(pq =>
        pq.toList.sorted.reverse.zipWithIndex.map {
          case ((item_name, count), idx) => (item_name, count, idx + 1)
        }
      )
      .map({ case (geographical_location_oid, (item_name, count, rank)) =>
        AggDetection(geographical_location_oid, item_name, count, rank)
      })

  }
  def join(x: RDD[AggDetection], y: RDD[Location]): RDD[Output] = {
    val xGrouped = x.map(d => (d.geographical_location_oid, d))
    val yGrouped = y.map(d => (d.geographical_location_oid, d))
    val joined = xGrouped.join(yGrouped) // can be left outer join
    joined.values
      .map({ case (agg, loc) =>
        Output(loc.geographical_location, agg.rank, agg.item_name)
      })
      .mapPartitions(x => {
        x.toSeq.sortBy(o => (o.geographical_location, o.item_rank)).iterator
      })
  }

  /** Custom joining without using `.join()` explicitly. Implemented by
    * converting RDD[Location] into a HashMap since Then broadcast this hashmap
    * to all the nodes and do lookups on this hashmap This is possible since
    * Location dataset is relatively small and doesn't grow much, so nodes won't
    * go out of mem.
    */
  def joinManual(
      spark: SparkSession,
      x: RDD[AggDetection],
      y: RDD[Location]
  ): RDD[Output] = {
    // Broadcast join since RDD[Location] is small and fixed size
    val iter = y.toLocalIterator.map { loc =>
      (loc.geographical_location_oid, loc.geographical_location)
    }.toSeq
    val locationHashMap: HashMap[BigInt, String] = HashMap(iter: _*)
    val locationHashMapBroadcast = spark.sparkContext.broadcast(locationHashMap)
    x.map({ case AggDetection(geog_oid, name, count, rank) =>
      Output(
        locationHashMapBroadcast.value
          .getOrElse(geog_oid, "Location Not Found"),
        rank,
        name
      )
    })
  }
}

