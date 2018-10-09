package dpla.ingestion3.utils

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.io.ByteArrayInputStream

import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, PutObjectResult}
import com.amazonaws.services.s3.AmazonS3Client

import scala.util.Try

/*
 * @param root: Root directory or AWS S3 bucket.
 *              If `root' is an AWS S3 bucket, it should start with "s3a://".
 * @param shortName: Provider short name
 * @param activity: "harvest", "mapping", "enrichment", etc.
 * @param startDateTime: start dateTime of the activity
 *
 * @throws IllegalArgumentException
 *
 * @see https://digitalpubliclibraryofamerica.atlassian.net/wiki/spaces/TECH/pages/84512319/Ingestion+3+Storage+Specification
 *      for details on file naming conventions
 *
 * The convention in this class is that methods with "path" in the name include
 * the root bucket/directory while methods with "key" do not.
 */
class OutputHelper(root: String,
                   shortName: String,
                   activity: String,
                   startDateTime: LocalDateTime) {

  /*
   * If root is an S3 bucket, ensure that s3a protocol is being used and not
   * s3 or s3n.
   */
  require(!root.startsWith("s3://") && !root.startsWith("s3n://"),
    "s3a protocol required for writing output")

  val timestamp: String = startDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

  /*
   * S3 bucket or root directory for output.
   * Includes trailing slash.
   * If S3 bucket, includes "s3a://" prefix.
   */
  private lazy val directory: String = if (root.endsWith("/")) root else s"$root/"

  /*
   * Activity directory.
   * Includes trailing slash.
   * Does not include starting "/"
   * For full directory path, including root directory/bucket, use `activityPath'
   */
  private lazy val activityDirectory: String = s"$shortName/$activity/"

  /*
   * Full path to activity directory.
   * Includes trailing slash.
   * Includes root directory or S3 bucket.
   */
  private lazy val activityPath: String = s"$directory$activityDirectory"

  /*
   * File name for a harvest, mapping, enrichment, jsonl.
   * For full output path, including root directory/bucket, use `filePath'
   * Does not include starting "/"
   *
   * Evaluate on instantiation so that invalid `activity' is caught immediately.
   */
  private lazy val fileKey: String = {

    // TODO: make schema configurable - could use sealed case classes for activities
    val schema: String = activity match {
      case "harvest" => "OriginalRecord"
      case "mapping" => "MAP4_0.MAPRecord"
      case "enrichment" => "MAP4_0.EnrichRecord"
      case "jsonl" => "MAP3_1.IndexRecord"
      case _ => throw new IllegalArgumentException(s"Activity '$activity' not recognized")
    }

    val fileType: String = if (activity == "jsonl") "jsonl" else "avro"

    s"$activityDirectory$timestamp-$shortName-$schema.$fileType"
  }

  /*
   * Full path to file for harvest, mapping, enrichment, jsonl.
   * Includes root directory/bucket.
   */
  private lazy val filePath: String = s"$directory$fileKey"

  /*
   * Full output path for any activity.
   *
   * @example:
   *   OutputHelper("s3a://dpla-master-dataset", "cdl", "harvest").outputPath =>
   *   "s3a://dpla-master-dataset/cdl/harvest/20170209_104428-cdl-OriginalRecord.avro"
   */
  lazy val outputPath: String = {
    if (activity == "reports") activityPath else filePath
  }

  /*
   * Parse S3 bucket name from given `root'.
   * Does not include trailing slash or "s3a://" prefix.
   * Returns empty string if unable to parse bucket name.
   */
  lazy val bucketName: String = Try{ directory.split("/")(2) }.getOrElse("")

  /*
   * Parse any directories nested under an S3 bucket.
   * Does not include leading slash.
   * Includes trailing slash.
   * @example if `root' = "s3://foo/bar/bat/" then `bucketNestedDir' = "bar/bat"
   */
  lazy val bucketNestedDir: String = directory.stripPrefix("s3a://")
    .stripPrefix(bucketName).stripPrefix("/")

  /*
   * Get path to manifest file, not including local root directory or s3 bucket.
   * Manifest will be in the same directory as activity output files.
   * Does not include starting "/".
   */
  lazy val manifestKey: String = s"$fileKey/_MANIFEST"

  /*
   * Get path to manifest with local root directory.
   */
  lazy val manifestLocalOutPath: String = s"$directory$manifestKey"

  /*
   * Get path to reports directory.
   * Include root bucket/directory and trailing "/".
   */
  lazy val logsBasePath: String = s"$directory$fileKey/_LOGS/"

  private lazy val s3client: AmazonS3Client = new AmazonS3Client
  private lazy val flatFileIO: FlatFileIO = new FlatFileIO

  /*
   * Write a manifest file in the given outputPath directory.
   *
   * @param outputPath: The directory in which the manifest file is to be written.
   * @param opts: Optional data points to be included in the manifest file.
   */
  def writeManifest(opts: Map[String, String]): Try[String] = {

    val text: String = manifestText(opts)

    if (outputPath.startsWith("s3a://"))
      writeS3File(bucketName, s"$bucketNestedDir$manifestKey", text)
    else
      writeLocalFile(manifestLocalOutPath, text)
  }

  /*
   * Create text for a manifest file.
   *
   * @param opts: Optional data points to be included in the manifest file.
   *              This is intentionally open-ended so that individual executors
   *              can include whatever data points are relevant to their activity.
   */
  private val manifestText: Map[String, String] => String = (opts: Map[String, String]) => {

    val date: String = startDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    // Add date/time to given `opts'
    val data: Map[String, String] = opts + ("Start date/time" -> date)

    data.map{ case(k, v) => s"$k: $v" }.mkString("\n")
  }

  /*
   * Write a String to a local file.
   *
   * @param outPath: Output path
   * @param text: Text string to be written to local file
   *
   * @return Try[String]: Path of output file.
   */
  def writeLocalFile(outPath: String, text: String): Try[String] = Try {
    flatFileIO.writeFile(text, outPath)
  }

  /*
   * Write a String to an S3 file.
   *
   * @param bucket: S3 bucket (do not include trailing slash or "s3a://" prefix)
   * @param key: S3 file key
   * @param text: Text string to be written to S3 file
   *
   * @return: Try[String] Path of written file.
   *          Identifier for specific version of the resource just written.
   */
  def writeS3File(bucket: String, key: String, text: String): Try[String] = Try {
    val in = new ByteArrayInputStream(text.getBytes("utf-8"))
    s3client.putObject(new PutObjectRequest(bucket, key, in, new ObjectMetadata))
    // Return filepath
    s"$bucket/$key"
  }
}
