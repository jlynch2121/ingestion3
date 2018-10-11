package dpla.ingestion3.dataStorage

import org.scalatest._

class DataStorageTest extends FlatSpec {

  val path = "s3a://my-bucket/foo/bar/"

  "parseS3Address" should "correctly parse an S3 protocol" in {
    assert(parseS3Address(path).protocol == "s3a")
  }

  "parseS3Address" should "correctly parse an S3 bucket" in {
    assert(parseS3Address(path).bucket == "my-bucket")
  }

  "parseS3Address" should "correctly parse an S3 prefix" in {
    assert(parseS3Address(path).prefix == Some("foo/bar"))
  }

  "parseS3Address" should "throw exception in absence of s3 protocol" in {
    val badPath = "/foo/bar"
    assertThrows[RuntimeException](parseS3Address(badPath))
  }

  "parseS3Address" should "throw exception in absence of bucket" in {
    val badPath = "s3a://"
    assertThrows[RuntimeException](parseS3Address(badPath))
  }
}