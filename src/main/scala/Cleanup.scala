import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import scala.util.{Try, Failure, Success}
import java.net.URL

// This selects and cleans up relevant data from the DPLA index data dump.

// This takes as input the output parquet file from:
//   https://github.com/dpla/analytics JsonDumpToJsonL
// It outputs a parquet file with the following columns:
//   name : String (name of the provider)
//   object : String (url of the thumbnail image)

// First argument: path to parquet file, eg. "allData.parquet"
// Second argument: destination path for output parquet file, 
//    eg. "cleanData.parquet"

// Example usage:
// PATH_TO_SPARK/bin/spark-submit --class "Cleanup" --master local[3] \
//   PATH_TO_THUMBNAIL_AUDIT_APP/target/scala-2.11/thumbnail-audit_2.11-1.0.jar \
//   INPUT_PATH OUTPUT_PATH


object Cleanup {

  val conf = new SparkConf().setAppName("Thumbnail Audit")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  def main(args: Array[String]) {

    val inputPath = args(0)
    val outputPath = args(1)

    // Read data from input file
    val data = sqlContext.read.parquet(inputPath)

    // Get provider name and image URL from items with thumbnails.
    val images = data.filter("_type = 'item' and _source.object is not null")
                     .select("_source.provider.name", "_source.object")
    
    // Remove duplicates.
    val uniqueImages = images.select("name","object").dropDuplicates

    // Clean up image URLs, and remove images with invalid URLs.
    // Note that `cleanUrlFunc` replaces invalid URLs with null, hence the need
    // to subsequently filter.
    val cleanImages = uniqueImages.withColumn("object", cleanUrlFunc(uniqueImages.col("object")))
                                  .filter("object is not null")

    // Save to file.
    cleanImages.write.parquet(outputPath)
    println(s"Cleaned-up data written to $outputPath")

    sc.stop()
  }

  // Return Some(String) or None.
  // This is defined as a function literal so it can be passed to `withColumn`.
  val cleanUrl: (String => Option[String]) = (url: String) => { 
    validUrl(stripArray(url))
  }
  val cleanUrlFunc = udf(cleanUrl)

  // Strip extra characters from URL Strings that represent arrays.
  // If multiple URLs are listed, it returns the first URL.
  // Example:
  //   stripArray("[\"http://example.com\"]") -> "http://example.com"
  def stripArray(url: String) : String = {
    def firstString(url: String) : String = {
      url.stripPrefix("[")
         .stripSuffix("]")
         .split(",")(0)
         .stripPrefix("\"")
         .stripSuffix("\"")
    }  
    if (url.startsWith("[")) firstString(url)
    else url
  }

  // Return Some(String) if the given URL is valid.
  // Otherwise return None.
  // Performing this cleanup step here will save time by avoiding any attempts
  // to request invalid URLs through HTTP.
  def validUrl(string: String) : Option[String] = {
    def url() : Try[URL] = Try {
      new URL(string) // with throw exception if string is not valid URL
    }

    url() match {
      case Success(_) => Some(string)
      case Failure(_) => None
    }
  }
}
