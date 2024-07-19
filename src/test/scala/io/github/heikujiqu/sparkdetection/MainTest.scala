package io.github.heikujiqu.sparkDetection

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

class TransformTest extends AnyFunSuite {
  var spark: SparkSession = _
  var sc: SparkContext = _

  override def withFixture(test: NoArgTest) = { // Define a shared fixture
    // Shared setup (run at beginning of each test)
    spark = SparkSession
      .builder()
      .appName("hello")
      .master("local")
      .config("spark.log.level", "WARN")// log less in tests
      .getOrCreate()
    sc = spark.sparkContext
    try test()
    finally {
      // Shared cleanup (run at end of each test)
      spark.stop()
    }
  }

  test ("VideoDetectionTransformations should remove duplicates, and return top 10 item names for each geographical location" ) {
    val detection = sc.parallelize(
      Seq(
        Detection(10, 1, 1, "a", 1),
        Detection(10, 1, 2, "a", 1),
        Detection(10, 1, 3, "a", 1),
        Detection(10, 1, 4, "b", 1),
        Detection(10, 1, 5, "b", 1),
        Detection(10, 1, 6, "c", 1),
        Detection(10, 1, 7, "c", 1),
        Detection(10, 1, 8, "d", 1),
        Detection(10, 1, 9, "d", 1),
        Detection(10, 1, 10, "e", 1),
        Detection(10, 1, 11, "e", 1),
        Detection(10, 1, 12, "f", 1),
        Detection(10, 1, 13, "f", 1),
        Detection(10, 1, 14, "g", 1),
        Detection(10, 1, 15, "g", 1),
        Detection(10, 1, 16, "h", 1),
        Detection(10, 1, 17, "h", 1),
        Detection(10, 1, 18, "i", 1),
        Detection(10, 1, 19, "i", 1),
        Detection(10, 1, 20, "j", 1),
        Detection(10, 1, 21, "j", 1),
        Detection(10, 1, 21, "j", 1), // intentional duplicated record, "j" should have 2 count still
        Detection(10, 1, 22, "excluded1", 1), // only got 1, not in top 10
        Detection(10, 1, 23, "excluded2", 1) // only got 1, not in top 10
      )
    )
    val location = sc.parallelize(
      Seq(Location(10, "location_name"))
    )
    val result = VideoDetectionTransformations.run(spark, detection, location, 10).collect()
    val expected = Array(
        Output("location_name",  1,"a"),
        Output("location_name",  2,"b"),
        Output("location_name",  3,"c"),
        Output("location_name",  4,"d"),
        Output("location_name",  5,"e"),
        Output("location_name",  6,"f"),
        Output("location_name",  7,"g"),
        Output("location_name",  8,"h"),
        Output("location_name",  9,"i"),
        Output("location_name",  10,"j"),
    )

    // expecte to have 10 items
    assert(result.length == 10)
    // expect all the item names "a" to "j" and their corresponding counts/rank to appear
    assert(result.map(_.item_name).toSet == expected.map(_.item_name).toSet)
    // and "excluded1" and "excluded2" should not appear
    assert(!result.map(_.item_name).toSet.contains("excluded1"))
    assert(!result.map(_.item_name).toSet.contains("excluded2"))
    // expected items should be sorted by rank, but same counts order is undefined
    // "a" should be first item because it has 3 counts
    // the sorting order of the other items are undefined as all of them have 2 counts so they are not tested
    assert(result(0) == expected(0))
  }
}
