package dpla.ingestion3.messages

// trait IngestErrorMessages {

trait IngestErrors {
  def mintUriError(id: String, msg: String, field: String, value: String): IngestMessage =
    IngestMessage(
      message = s"Unable to mint URI: $msg",
      id = id,
      field = field,
      value = value
    )

  def missingRequiredError(id: String, field: String): IngestMessage =
    IngestMessage(
      message = s"Missing required field",
      level = "ERROR",
      id = id,
      field = field,
      value = "MISSING"
    )
}