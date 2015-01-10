package stoop

import spray.json._
import dispatch._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.ning.http.client.Response
import DefaultJsonProtocol._

/**
 * An attempt to put together a more natural interface that is still functional for couch.
 */
case class IdRevReader(id: String, rev: String)
object IdRevReader {
  implicit val jsonFormat = jsonFormat2(IdRevReader.apply)
}

trait HasId {
  def _id: String
}

trait FlowDB {
  def get[A : JsonReader](id: String)(fn: Option[A] => Unit)
  def getAll[A : JsonFormat](ids: Seq[String])(fn: Seq[A] => Unit)
  def create[A <: HasId : JsonFormat](obj: A)(fn: A => Unit)
  def update[A <: HasId : JsonFormat](obj: A)(fn: A => Unit)
  def createAll[A <: HasId : JsonFormat](objs: Seq[A])(fn: Seq[A] => Unit)
  def updateAll[A <: HasId : JsonFormat](objs: Seq[A])(fn: Seq[A] => Unit)
  def queryForType[A : JsonReader](query: String)(fn: Seq[A] => Unit)
}

case class DBInfo(server: String, dbName: String) extends FlowDB with FieldTweaker {
  val dbTemplate = CouchDBTemplate(server, dbName)

  private def fixField(fields: Map[String, JsValue], replace: Map[String, String]): Map[String, JsValue] = {
    val withExtras = fields ++ (for {
      (from, to) <- replace
      if fields.contains(from)
      value = fields(from)
      if !value.convertTo[String].isEmpty
    } yield to -> value)

    withExtras -- replace.keys
  }

  def get[A: JsonReader](id: String)(fn: Option[A] => Unit) {
    val result = try {
      Some(Dispatcher.getType[A](dbTemplate.forId(id)))
    }
    catch {
      case ex: Exception => None
    }
    fn(result)
  }

  def getAll[A: JsonFormat](ids: Seq[String])(fn: Seq[A] => Unit) {
    val idsJson = ids.toJson.toString()
    val results = Dispatcher.postReadType[Vector[A]](dbTemplate.forAllDocs, idsJson)
    fn(results)
  }

  def create[A <: HasId : JsonFormat](obj: A)(fn: A => Unit) {
    val idRev = if (obj._id == null || obj._id.isEmpty) {
      Dispatcher.postReadType[IdRevReader](dbTemplate.dbURL, fixFieldsForWrite(obj.toJson).toString())
    } else {
      Dispatcher.putReadType[IdRevReader](dbTemplate.forId(obj._id), fixFieldsForWrite(obj.toJson).toString())
    }
    fn(Dispatcher.getType[A](dbTemplate.forId(idRev.id)))
  }

  def update[A <: HasId : JsonFormat](obj: A)(fn: A => Unit) {
    require(obj._id != null && !obj._id.isEmpty, "May not update an object without valid ID field")
    val idRev = Dispatcher.putReadType[IdRevReader](dbTemplate.forId(obj._id), fixFieldsForWrite(obj.toJson).toString())
    fn(Dispatcher.getType[A](dbTemplate.forId(idRev.id)))
  }

  def createAll[A <: HasId : JsonFormat](objs: Seq[A])(fn: Seq[A] => Unit) {

  }

  def updateAll[A <: HasId : JsonFormat](objs: Seq[A])(fn: Seq[A] => Unit) {

  }

  def queryForType[A : JsonReader](query: String)(fn: Seq[A] => Unit) {

  }
}

trait FieldTweaker {
  private def removeEmpties(fields: Map[String, JsValue], emptiesToRemove: Seq[String]) = {
    fields -- (for {
      noEmpty <- emptiesToRemove
      if fields.contains(noEmpty) && fields(noEmpty).convertTo[String].isEmpty
    } yield noEmpty)
  }

  protected def fixFieldsForWrite(jsValue: JsValue): JsValue = jsValue match {
    case JsObject(fields) => JsObject(removeEmpties(fields, Seq("id", "_id", "rev", "_rev")))
    case x => x
  }
}

trait FlowAdmin {
  def createDb[A](name: String)(fn: DBInfo => A): A
  def dropDb[A](name: String)(fn: Boolean => A): A
  def listDbs[A](fn: Seq[DBInfo] => A): A
  def withDb[A](name: String)(fn: DBInfo => A): A
  def withDb[A](db: DBInfo)(fn: DBInfo => A): A
}

case class DbAdmin(server: String) extends FlowAdmin {
  val serverTemplate = CouchServerTemplate(server)

  def createDb[A](name: String)(fn: DBInfo => A): A = {
    if (Dispatcher.putSuccess(serverTemplate.forDBName(name))) {
      fn(DBInfo(server, name))
    } else failed(s"Unable to create DB named $name on server $server")
  }

  def dropDb[A](name: String)(fn: (Boolean) => A): A = {
    val worked = Dispatcher.deleteSuccess(serverTemplate.forDBName(name))
    fn(worked)
  }

  def listDbs[A](fn: (Seq[DBInfo]) => A): A = {
    val dbs = Dispatcher.getType[List[String]](serverTemplate.listDBs)
    val dbList = dbs map { dbString => DBInfo(server, dbString) }
    fn(dbList)
  }

  def withDb[A](name: String)(fn: (DBInfo) => A): A = {
    val db = DBInfo(server, name)
    fn(db)
  }

  def withDb[A](db: DBInfo)(fn: (DBInfo) => A): A = fn(db)

  def failed(msg: String) = throw new RuntimeException(msg)
}

object Dispatcher {
  import scala.concurrent.ExecutionContext.Implicits.global

  val standardTimeout = Duration(20, TimeUnit.SECONDS)
  val longTimeout = Duration(200, TimeUnit.SECONDS)
  val reallyLongTimeout = Duration(2000, TimeUnit.SECONDS)

  def getType[A : JsonReader](req: String): A = {
    val result = get(req)
    result.getResponseBody.asJson.convertTo[A]
  }

  def getSuccess(req: String): Boolean = isSuccess(get(req))

  def putReadType[A : JsonReader](req: String, content: String = ""): A = {
    val result = put(req, content)
    result.getResponseBody.asJson.convertTo[A]
  }

  def putSuccess(req: String, content: String = ""): Boolean = isSuccess(put(req, content))

  def postReadType[A : JsonReader](req: String, content: String = ""): A = {
    val result = post(req, content)
    result.getResponseBody.asJson.convertTo[A]
  }

  def postSuccess(req: String, content: String = ""): Boolean = isSuccess(post(req, content))

  def deleteReadType[A : JsonReader](req: String): A = {
    val result = delete(req)
    result.getResponseBody.asJson.convertTo[A]
  }

  def deleteSuccess(req: String): Boolean = isSuccess(delete(req))

  def get(req: String, timeout: Duration = standardTimeout): Response = {
    val todo = url(req)
    Await.result(Http(todo), timeout)
  }

  def put(req: String, content: String = "", timeout: Duration = standardTimeout): Response = {
    val todo = url(req).PUT << content
    Await.result(Http(todo), timeout)
  }

  def post(req: String, content: String = "", timeout: Duration = standardTimeout): Response = {
    val todo = url(req).POST
    Await.result(Http(todo), timeout)
  }

  def delete(req: String, timeout: Duration = standardTimeout): Response = {
    val todo = url(req).DELETE
    Await.result(Http(todo), timeout)
  }

  def isSuccess(response: Response): Boolean = response.getStatusCode >= 200 && response.getStatusCode < 300
}
