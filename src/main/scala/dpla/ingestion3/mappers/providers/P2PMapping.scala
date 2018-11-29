package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.NodeSeq

class P2PMapping()
  extends XmlMapping
    with XmlExtractor
    with IdMinter[NodeSeq]
    with IngestMessageTemplates {

  // ID minting functions
  override def useProviderName: Boolean = true

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: String = "p2p"

  override def getProviderId(implicit data: Document[NodeSeq]): String =
    extractString(data \\ "header" \ "identifier")
    .map(_.trim)
      .getOrElse(throw new RuntimeException(s"No ID for record $data"))

  // OreAggregation fields
  override def dplaUri(data: Document[NodeSeq]): ExactlyOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    for {
      node <- data \ "metadata" \ "mods" \ "note"
      if node \@ "type" == "ownership"
    } yield nameOnlyAgent(node.text.trim)

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
    (for {
      node <- data \ "metadata" \ "mods" \ "note"
      if node \@ "type" == "admin"
    } yield nameOnlyAgent(node.text.trim)).headOption

  //<mods:accessCondition type="use and reproduction">
  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    for {
      node <- data \ "metadata" \ "mods" \ "accessCondition"
      if node \@ "type" == "use and reproduction"
    } yield URI(node.text.trim)

  /*
    <mods:location>
    <mods:url access=”object in context” usage="primary display">
  */
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    for {
      node <- data \ "metadata" \ "mods" \ "location" \ "url"
      if node \@ "access" == "object in context"
      if node \@ "usage" == "primary display"
    } yield uriOnlyWebResource(URI(node.text.trim))

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  //<mods:location><mods:url access="preview">
  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    for {
      node <- data \ "metadata" \ "mods" \ "location" \ "url"
      if node \@ "access" == "preview"
    } yield uriOnlyWebResource(URI(node.text.trim))

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("Plains to Peaks Collective"),
    uri = Some(URI("http://dp.la/api/contributor/p2p"))
  )

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    for {
      name <- data \ "metadata" \ "mods" \ "name"
      if (name \ "role" \ "roleTerm").text.trim == "contributor"
    } yield nameOnlyAgent((name \ "namePart").text.trim)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    for {
      name <- data \ "metadata" \ "mods" \ "name"
      if (name \ "role" \ "roleTerm").text.trim == "creator"
    } yield nameOnlyAgent((name \ "namePart").text.trim)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "metadata" \ "mods" \ "originInfo" \ "dateCreated")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "abstract")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "physicalDescription" \ "extent")

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "recordInfo" \ "recordIdentifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "mods" \ "language" \ "languageTerm")
      .map(nameOnlyConcept)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    for {
      subjectNode <- data \ "metadata" \ "mods" \ "subject"
      subject <-
        subjectNode \ "topic" ++
          subjectNode \ "name" ++
          subjectNode \ "genre"
    } yield nameOnlyConcept(subject.text.trim)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \ "metadata" \ "mods" \ "titleInfo" \ "title")

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "typeOfResource")

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "mods" \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "physicalDescription" \ "form")

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \ "metadata" \ "mods" \ "subject" \ "geographic")
      .map(nameOnlyPlace)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    for {
      relatedItem <- data \ "metadata" \ "mods" \ "relatedItem"
      if relatedItem \@ "type" == "series"
      relation <- relatedItem \ "titleInfo" \ "title"
    } yield Left(relation.text.trim)

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    for {
      relatedItem <- data \ "metadata" \ "mods" \ "relatedItem"
      if relatedItem \@ "type" == "host"
      collectionTitle <- relatedItem \ "titleInfo" \ "title"
    } yield nameOnlyCollection(collectionTitle.text.trim)

}
