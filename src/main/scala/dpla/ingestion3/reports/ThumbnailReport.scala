package dpla.ingestion3.reports

import dpla.ingestion3.model.DplaMapData
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.util.{Try, Failure, Success}
import javax.imageio.ImageIO
import java.net.URI

/**
  * Produces QA reports related to thumbnails (i.e. oreAggregation.preview).
  *
  * The "missing" report (i.e. params = "missing") returns all images that are
  * missing thumbnails.
  * The resulting Dataframe has the following columns:
  *   - localUri: String, the provider URI for the item
  *   - dplaUri: String, DPLA's URI for the item
  *
  * The "dimensions" report (i.e. params = "dimensions") returns the height and
  * width of each image, plus any errors encountered when attempting to call the
  * URI endpoint.  Please note that this report takes a long time to run, as it
  * has to make an HTTP request for every thumbnail in the data sample.
  * The resulting Dataframe has the following columns:
  *    - localUri: String, the provider URI for the item
  *    - dplaUri: String, DPLA's URI for the item
  *    - preview: String, thumbnail URI
  *    - height: Int, height in pixels of the image
  *    - width: Int, width in pixels of the image
  *    - error: String, any error that occurred while attempting to call the
  *                     thumbnail URI and get its dimensions
  *
  * @param inputURI String, path to the data sample
  * @param outputURI String, path to write output
  * @param sparkMasterName String
  * @param params Array[String], string value can be "missing" OR "dimensions"
  */
class ThumbnailReport (
                            val inputURI: String,
                            val outputURI: String,
                            val sparkMasterName: String,
                            val params: Array[String]) extends Report with Serializable {

  override val sparkAppName: String = "ThumbnailReport"

  override def getInputURI: String = inputURI

  override def getOutputURI: String = outputURI

  override def getSparkMasterName: String = sparkMasterName

  override def getParams: Option[Array[String]] = {
    params.nonEmpty match {
      case true => Some(params)
      case _ => None
    }
  }

  /**
    * Process the incoming dataset (mapped or enriched records) and return a
    * DataFrame of computed results.
    *
    * @param ds    Dataset of DplaMapData (mapped or enriched records)
    * @param spark The Spark session, which contains encoding / parsing info.
    * @return DataFrame, typically of Row[value: String, count: Int]
    */
  override def process(ds: Dataset[DplaMapData], spark: SparkSession): DataFrame = {


    implicit val dplaMapDataEncoder =
      org.apache.spark.sql.Encoders.kryo[DplaMapData]

    val token: String = getParams match {
      case Some(p) => p.headOption.getOrElse("")
      case _ => throw new RuntimeException("No thumbnail report type specified.")
    }

    token match {
      case "missing" => missingReport(ds, spark)
      case "dimensions" => dimensionsReport(ds, spark)
      case x => throw new RuntimeException(s"Unrecognized thumbnail report name '${x}'")
    }

  }

  /**
    * Get images with missing thumbnails.
    * Images are items where sourceResource.`type` = image
    */
  def missingReport(ds: Dataset[DplaMapData], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val thumbnailData: Dataset[MissingThumbnail] = ds.map(dplaMapData => {

      val hasImageType = dplaMapData.sourceResource.`type`
        .filter {  _.toLowerCase() == "image" }
        .nonEmpty

      val hasPreview = previewUri(dplaMapData).nonEmpty

      MissingThumbnail(
        localUri(dplaMapData),
        dplaUri(dplaMapData),
        hasImageType,
        hasPreview)
    })

    thumbnailData.select("localUri", "dplaUri")
      .filter("hasImageType = TRUE")
      .filter("hasPreview = FALSE")
  }

  /**
    * Get thumbnail dimensions, and any errors that occur when attempting a
    * request the thumbnail endpoint.
    * Warning: This process takes a long time to run.
    */
  def dimensionsReport(ds: Dataset[DplaMapData], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val thumbnailData: Dataset[ThumbnailDimensions] = ds.map(dplaMapData => {

      val uri: Option[URI] = previewUri(dplaMapData)

      val preview: Option[String] = uri.map(_.toString)

      val dimensions: Option[Dimensions] = uri.map(getDimensions)

      ThumbnailDimensions(
        localUri(dplaMapData),
        dplaUri(dplaMapData),
        preview,
        dimensions
      )
    })

    thumbnailData.select("localUri", "dplaUri", "preview", "dimensions.height",
      "dimensions.width", "dimensions.error")
  }

  /**
    * @param dplaMapData
    * @return String, the DPLA URI for an item
    */
  def localUri(dplaMapData: DplaMapData): String =
    dplaMapData.edmWebResource.uri.toString

  /**
    * @param dplaMapData
    * @return String, the provider URI for an item
    */
  def dplaUri(dplaMapData: DplaMapData): String =
    dplaMapData.oreAggregation.uri.toString

  /**
    * @param dplaMapData
    * @return Option[URI], the thumbnail URI
    */
  def previewUri(dplaMapData: DplaMapData): Option[URI] =
    dplaMapData.oreAggregation.preview.map(_.uri)

  /**
    * Parse the height and width from the thumbnail image.
    *
    * @param uri URI for the thumbnail
    * @return Dimensions case class containing the height and width, or an error
    *         message if an error occurs.
    */
  def getDimensions(uri: URI): Dimensions = {
    bufferedImg(uri) match {
      case Success(bimg) =>
        // Checking for null here creates a codacy error, but since BufferedImage
        // is a Java class, it can legitimately be null.
        if (bimg == null) Dimensions(error = Some("Buffered image is null."))
        else Dimensions(height = Some(bimg.getHeight), width = Some(bimg.getWidth))
      case Failure(err) => Dimensions(error = Some(err.getMessage))
    }
  }

  /**
    * Attempt to get the thumbnail image over HTTP.
    *
    * @param uri URI for the thumbnail
    * @return Success(BufferedImage) if attempt to read image over HTTP is successful.
    *         Otherwise returns Failure(Exception)
    */
  def bufferedImg(uri: URI) : Try[java.awt.image.BufferedImage] = Try {
    ImageIO.read(uri.toURL)
  }
}

case class MissingThumbnail(localUri: String,
                            dplaUri: String,
                            hasImageType: Boolean,
                            hasPreview: Boolean)

case class ThumbnailDimensions(localUri: String,
                               dplaUri: String,
                               preview: Option[String],
                               dimensions: Option[Dimensions])

case class Dimensions(height: Option[Int] = None,
                      width: Option[Int] = None,
                      error: Option[String] = None)