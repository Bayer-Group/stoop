package stoop

import spray.json._

sealed trait CouchView {
  def name: String
  def map: String
}
case class CouchMap(name: String, map: String) extends CouchView
case class CouchMapReduce(name: String, map: String, reduce: String) extends CouchView

object CouchView {
  implicit val couchViewWriter: JsonWriter[CouchView] = new JsonWriter[CouchView] {
    import DefaultJsonProtocol._
    def write(view: CouchView) = view match {
      case CouchMap(db, fn) =>
        JsObject(Map("map" -> JsString(fn)))
      case CouchMapReduce(db, map, reduce) =>
        JsObject(Map("map" -> JsString(map), "reduce" -> JsString(reduce)))
    }
  }
}
