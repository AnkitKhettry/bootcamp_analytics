object CaseClasses {

  case class EventSchema(timestamp: Long, eventType: String, sessionID: Long, brand: String, os: String){
    override def toString: String = productIterator.mkString(",")
  }
}
