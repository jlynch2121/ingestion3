package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model.DplaMapData.ExactlyOne
import dpla.ingestion3.model._

import scala.util.Try
import scala.xml._

class WiExtractor(rawData: String) extends Extractor with XmlExtractionUtils {

  implicit val xml: NodeSeq = XML.loadString(rawData)

  def agent = EdmAgent(
    name = Some("Recollection Wisconsin"),
    uri = Some(new URI("http://dp.la/api/contributor/wisconsin"))
  )

  def dataProvider(): ExactlyOne[EdmAgent] = {
    extractString(xml \ "metadata" \\ "dataProvider") match {
      case Some(provider) => nameOnlyAgent(provider)
      case None => throw new Exception("Missing required property dataProvider")
    }
  }
  // Get the second occurrence of the dc:identifier property
  def itemUri(): ExactlyOne[URI] = {
    extractString(xml \ "metadata" \\ "isShownAt") match {
      case Some(uri) => new URI(uri)
      case None => throw new Exception("Missing required property isShownAt")
    }
  }

  def build(): Try[DplaMapData] = Try {
    DplaMapData(
      DplaSourceResource(
        alternateTitle = extractStrings(xml \ "metadata" \\ "alternative"),
        // This method of using NodeSeq is required because of namespace issues.
        collection = extractStrings(xml \ "metadata" \\ "isPartOf").headOption.map(nameOnlyCollection).toSeq,
        contributor = extractStrings(xml \ "metadata" \\ "contributor").dropRight(1).map(nameOnlyAgent),
        creator = extractStrings(xml \ "metadata" \\ "creator").map(nameOnlyAgent),
        date = extractStrings(xml \ "metadata" \\ "date").map(stringOnlyTimeSpan),
        description = extractStrings(xml \ "metadata" \\ "description"),
        format = extractStrings(xml \ "metadata" \\ "format").filterNot(isDcmiType) ++
          extractStrings(xml \ "metadata" \\ "medium").filterNot(isDcmiType) ,
        identifier = extractStrings(xml \ "metadata" \\ "identifier"),
        language = extractStrings(xml \ "metadata" \\ "language").map(nameOnlyConcept),
        place = extractStrings(xml \ "metadata" \\ "spatial").map(nameOnlyPlace),
        publisher = extractStrings(xml \ "metadata" \\ "publisher").map(nameOnlyAgent),
        relation = extractStrings(xml \ "metadata" \\ "relation").map(eitherStringOrUri),
        rights = extractStrings(xml \ "metadata" \\ "rights") ++
          extractStrings(xml \ "metadata" \\ "accessRights"),
        rightsHolder = extractStrings(xml \ "metadata" \\ "rightsHolder").map(nameOnlyAgent),
        subject = extractStrings(xml \ "metadata" \\ "subject").map(nameOnlyConcept),
        title = extractStrings(xml \ "metadata" \\ "title"),
        `type` = extractStrings(xml \ "metadata" \\ "type").filter(isDcmiType)
      ),

      EdmWebResource(
        uri = itemUri
      ),

      OreAggregation(
        uri = mintDplaItemUri(),
        dataProvider = dataProvider(),
        originalRecord = rawData,
        provider = agent,
        preview = extractString(xml \ "metadata" \\ "preview").map(x => uriOnlyWebResource(new URI(x)))
      )
    )
  }

  override def getProviderBaseId(): Option[String] = extractString(xml \ "header" \ "identifier")
}