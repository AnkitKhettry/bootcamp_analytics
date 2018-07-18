object CaseClasses {

  case class EventSchema(timestamp: Long, eventType: String, sessionID: Long, brand: String, os: String) extends Serializable {
    override def toString: String = productIterator.mkString(",")
  }
}
