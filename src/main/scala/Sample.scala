import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// import org.apache.spark.storage.StorageLevel
import scala.util.{Try, Failure, Success}
import javax.imageio.ImageIO
import java.net.URL

// This takes a stratfied sample of image URLs by provider.

// It takes as input the output parquet file from Cleanup class.
// It outputs a parquet file with the following columns:
//   name : String (name of the provider)
//   url : String (url of the thumbnail image)
//   height : Int (height of the thumbnail image in pixels)
//   width : Int (witdth of the thumbnail image in pixels)

// First argument: path to parquet file containing images, eg. "allData.parquet"
// Second argument: destination path for output parquet file, eg. "twoPercentSample.parquet"
// Third argument: sample size, eg. "0.02"

// Example usage:
// PATH_TO_SPARK/bin/spark-submit --class "Sample" --master local[3] PATH_TO_THUMBNAILS_APP/target/scala-2.11/thumbnails-project_2.11-1.0.jar INPUT_PATH OUTPUT_PATH 0.02

object Sample {

  val conf = new SparkConf().setAppName("Thumbnails Application")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  case class Image(url: String, provider: String, width: Int, height: Int)

  def main(args: Array[String]) {

    val inputPath = args(0)
    val outputPath = args(1)
    val sampleSize = args(2).toDouble

    val data = sqlContext.read.parquet(inputPath)
    // data.persist(StorageLevel.DISK_ONLY)

    // Translate input DataFrame to RDD.
    val dataRdd = data.rdd.map(x => (x(0).asInstanceOf[String], x(1).asInstanceOf[String]))

    // Get stratfied sample.
    println("Getting stratified sample...")
    val sample = stratifiedSample(dataRdd, sampleSize)

    // Get image dimensions.
    // Any image URLs that fail to get dimensions are removed from sample.
    println("Getting image dimensions...")
    val images = sample.flatMap { case (k, v) => constructImage(k, v) }
    val imagesDataFrame = sqlContext.createDataFrame(images)

    // Save to file.
    imagesDataFrame.write.parquet(outputPath)
    println(s"Sample written to $outputPath")

    sc.stop()
  }

  // Return the specified percentage image URLs for each provider.
  // The sample size for each provider is approximate, not exact.
  def stratifiedSample(dataRdd: org.apache.spark.rdd.RDD[(String, String)], sampleSize: Double): org.apache.spark.rdd.RDD[(String, String)] = {
    val fractions = dataRdd.keys.distinct.map(x => (x, sampleSize)).collect().toMap
    dataRdd.sampleByKey(withReplacement = false, fractions = fractions)
  }

  // Return Some(Image) if attempt to get dimensions is successful.
  // Otherwise return None.
  def constructImage(provider: String, url: String) : Option[Image] = {
    bufferedImg(url) match {
      case Success(bimg) =>
        if (bimg == null) return None
        Some(Image(url, provider, bimg.getWidth, bimg.getHeight))
      case Failure(_) => None
    }
  }

  // Return Success(BufferedImage) attempt to read image over HTTP is successful.
  // Otherwise returns Failure(Exception)
  def bufferedImg(url: String) : Try[java.awt.image.BufferedImage] = Try {
    ImageIO.read(new URL(url))
  }
}
