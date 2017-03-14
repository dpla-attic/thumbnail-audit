package la.dp.thumbnailaudit

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf

// This finds the longest edge for each thumbnail and writes it as csv.

// This takes as input the output parquet file from:
//   https://github.com/dpla/analytics Sample
// It outputs a directory containing a single csv file with the following
// columns:
//   provider : String (name of the provider)
//   longest_edge : Int (longest edge in pixels of the thumbnail image)

// First argument: path to parquet file, eg. "sample.parquet"
// Second argument: destination path for output csv directory eg. "longestEdge"

// Example usage:
// PATH_TO_SPARK/bin/spark-submit --class "la.dp.thumbnailaudit.Longestedge" \
//   --master local[3] \
//   PATH_TO_THUMBNAIL_AUDIT_APP/target/scala-2.11/thumbnail-audit_2.11-1.0.jar \
//   INPUT_PATH OUTPUT_PATH


object Longestedge {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Thumbnail Audit")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val inputPath = args(0)
    val outputPath = args(1)

    // Read data from input file.
    val data = sqlContext.read.parquet(inputPath)

    // Get longest edges.
    val longestEdges = data.withColumn("longest_edge",
                                       longestFunc(data.col("width"),
                                       data.col("height")))

    // Write to CSV.
    longestEdges.select("provider", "longest_edge")
                .coalesce(1)
                .write
                .format("csv")
                .option("header", "true")
                .save(outputPath)

    println(s"Longest edge data written to $outputPath")
    sc.stop()
  }

  // Return Int or None.
  // This is defined as a function literal so it can be passed to `withColumn`.
  val longest = (edge1: Int, edge2: Int) => { 
    if (edge1 > edge2) edge1
    else edge2
  }
  val longestFunc = udf(longest)
}
