package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class P2PMappingTest extends FlatSpec with BeforeAndAfter {

  val shortName = "p2p"
  val jsonString: String = new FlatFileIO().readFileAsString("/p2p.json")
  val json: JValue = parse(jsonString)
  val extractor = new P2PMapping

  it should "extract provider id" in {
    val expected = "https://plains2peaks.org/0a00aa9c-0b9e-11e8-a081-005056c00008"
    assert(extractor.getProviderId(Document(json)) === expected)
  }
  it should "extract alternate titles" in {
    val expected = Seq("Alt title 1")
    assert(extractor.alternateTitle(Document(json)) === expected)
  }
  it should "extract contributor" in {
    val expected = Seq("Cobos, Ruben").map(nameOnlyAgent)
    assert(extractor.contributor(Document(json)) === expected)
  }
  it should "extract creator" in {
    val expected = Seq("Berry, Edwin").map(nameOnlyAgent)
    assert(extractor.creator(Document(json)) === expected)
  }
  it should "extract date" in {
    val expected = Seq("20th century").map(stringOnlyTimeSpan)
    assert(extractor.date(Document(json)) === expected)
  }
  it should "extract description" in {
    val expected = Seq("Tome, New Mexico. No listing in Cobos index. Quality: good/fair. PLEASE NOTE: this should be number 15 of 17 songs on the audiofile.")
    assert(extractor.description(Document(json)) === expected)
  }
}