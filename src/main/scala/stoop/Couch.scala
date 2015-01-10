package stoop

import spray.json._
import DefaultJsonProtocol._
import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import Free.{freeMonad => _, _}
import stoop.attachments.Attachment
import java.util.UUID.randomUUID
import java.nio.charset.Charset
import java.nio.charset.Charset.defaultCharset
import java.io._

/** A database name. Provides combinators that work on a particular database. */
case class DB(name: String) {
  implicit val BadExecutionContext: scala.concurrent.ExecutionContext = null
  if (!isDBName(name)) sys.error(s"invalid database name: $name")

  /** A pure value that requires this database name but does nothing with it. */
  def pure[A](a: A): Couch[A] = Free.pure(a)

  /**
   * Creates a new document. Returns an error or the created revision and a boolean.
   * The boolean indicates whether the response was 202, which may need special handling.
   */
  def sensitiveNewNamedDoc[A:JsonWriter](doc: Option[Doc], json: A, params: Map[String, JsValue] = Map.empty): Couch[String \/ (Doc, Rev, Boolean)] =
    liftF(NewDoc(this, doc, json.toJson, params, a => a))

  /**
   * Creates a new document with the given name and contents.
   */
  def newNamedDoc[A:JsonWriter](doc: Doc, json: A, params: Map[String, JsValue] = Map.empty): Couch[String \/ (Doc, Rev)] =
    sensitiveNewNamedDoc(Some(doc), json, params).map(_ map (t => t._1 -> t._2))

  /** Update a document revision */
  def updateDoc[A:JsonWriter](doc: Doc, rev: Rev, json: A, params: Map[String, JsValue] = Map.empty): Couch[Option[(Doc, Rev)]] =
    liftF(UpdateDoc(this, doc, rev, json.toJson, params, a => a))

  /** Bulk-update documents. Revisions and IDs should be part of the JSON for each doc. */
  def bulkUpdateDocs[A:JsonWriter](jsons: Seq[A], params: Map[String, JsValue] = Map.empty): Couch[Option[List[BulkOperationFailure \/ BulkOperationSuccess]]] =
    liftF(BulkUpdateDocs(this, jsons.map(_.toJson), params, a => a))

  /**
   * Delete a document by docID, assuming the latest revision.
   * Returns false if the document doesn't exist or if there's a conflict.
   */
  def forceDeleteDoc(doc: Doc): Couch[Boolean] = for {
    r <- getDocPrim(doc)
    b <- r match {
      case Some((d, rev, _)) => deleteDoc(d, rev)
      case None => Monad[Couch].pure(false)
    }
  } yield b

  private def freshName = Doc(randomUUID().toString.replaceAll("-", ""))

  /** Gets a document as a raw map of JSON values. */
  def getDocPrim(doc: Doc, params: Map[String, JsValue] = Map.empty): Couch[Option[(Doc, Rev, Map[String, JsValue])]] =
    liftF(GetDoc(this, doc, params, x => x map(_.map(_.asJsObject.fields))))

  /** Delete a document given a particular revision. */
  def deleteDoc(doc: Doc, rev: Rev, params: Map[String, JsValue] = Map.empty): Couch[Boolean] =
    liftF(DeleteDoc(this, doc, rev, params, a => a))

  /** Create a new document with a generated name. */
  def newDoc[A:JsonWriter](json: A, params: Map[String, JsValue] = Map.empty): Couch[(Doc, Rev)] = {
    // The CouchDB specification recommends that clients generate their own UUIDs
    sensitiveNewNamedDoc(None, json, params) map { x =>
      x.toOption.map(t => t._1 -> t._2) getOrElse
        sys.error("Unable to create new document with generated name. UUID not unique?")
    }
  }

  /** Retrieve the given document. */
  def getDoc[A:JsonReader](doc: Doc, params: Map[String, JsValue] = Map.empty): Couch[Option[(Doc, Rev, A)]] =
    getDocPrim(doc, params).map { _ map {
      case (d, r, j) => (d, r, j.toJson.convertTo[A])
    }}

  /** Retrieve all documents matching the query parameters and deal with them as raw json rows */
  def getAllDocsRaw[A:JsonReader](params: Map[String, JsValue], body: Option[JsValue] = None): Couch[List[A]] =
    liftF(GetAllDocsRaw(this, params, body, (x: List[JsValue]) => x.map(_.convertTo[A])))

  /**
   * Modify the given document with the given update function
   * and return the revision number of the result if the update succeeds.
   */
  def getAndUpdateDoc[A:JsonFormat](doc: Doc, f: A => A, params: Map[String, JsValue] = Map.empty): Couch[Option[Rev]] = (for {
    r <- OptionT(getDoc[A](doc, params))
    (id, rev, a) = r
    s <- OptionT(updateDoc(id, rev, f(a), params))
  } yield s._2).run

  /** Get the IDs of all the documents in the database. */
  def getAllDocIDs(includeDesignDocs: Boolean = false, params: Map[String, JsValue] = Map.empty): Couch[List[Doc]] =
    liftF(GetAllDocIDs(this, params, identity, includeDesignDocs))

  /** Create views under the given design doc */
  def newView(design: String, views: Seq[CouchView], language: String = "javascript"): Couch[Unit] = {
    val content = views.map(x => x.name -> x.toJson).toMap
    val body: JsValue = JsObject(Map("language" -> JsString(language),
                                     "views"    -> JsObject(content)))
    val path = "_design/" + design
    def update(x: JsValue): JsValue = (x.asJsObject.fields.map {
      case ("views", JsObject(v)) => ("views", JsObject(content ++ v):JsValue)
      case _ => sys.error(s"Expected views to exist in design doc $design.")
    }).toJson

    for {
      r <- newNamedDoc(Doc(path), body)
      _ <- r.fold(
        _ => getAndUpdateDoc(Doc(path), update).map(
               _.getOrElse(sys.error("Creation of the design document $design failed."))),
        _ => commit)
    } yield ()
  }

  /** Returns the documents matching the given query. */
  def queryView[A:JsonReader](design: Doc, view: Doc, params: Map[String, JsValue] = Map()): Couch[Map[Option[Doc], A]] =
    liftF(QueryViewObjects(this, design, view, params,
      (x: List[JsValue]) => x.map(row => toRow(row.asJsObject, "value")).
        map(tup => tup._1.map(Doc.apply) -> tup._2).toMap.mapValues(_.convertTo[A])
    ))


  def queryViewDocs[A:JsonReader](design: Doc, view: Doc, params: Map[String, JsValue] = Map("include_docs" -> JsBoolean(true))): Couch[Map[Option[Doc], A]] =
    liftF(QueryViewObjects(this, design, view, params,
      (x: List[JsValue]) => x.map(row => toRow(row.asJsObject, "doc")).
        map(tup => tup._1.map(Doc.apply) -> tup._2).toMap.mapValues(_.convertTo[A])
    ))


  /** Returns only the IDs of the documents matching the given query. */
  def queryViewKeys(design: Doc, view: Doc, params: Map[String, JsValue] = Map()): Couch[List[Doc]] =
    liftF(QueryViewKeys(this, design, view, params, identity))

  /** Returns a list of raw JSON objects which must be interpreted by the caller - the most general query results.*/
  def queryViewObjects[A:JsonReader](design: Doc, view: Doc, params: Map[String, JsValue] = Map()): Couch[List[A]] =
    liftF(QueryViewObjects(this, design, view, params, (x: List[JsValue]) => x.map (_.convertTo[A])))

  /**
   * Get the given attachment from the given document, reading it through the given `Channel`.
   * The `Channel` is essentially a handler for each chunk of bytes of size `bufferSize`.
   * It will generate values that at the end get combined with a monoid.
   * In practice, this monoid is usually the trivial `Unit` monoid, if the channel writes to a file.
   */
  def getBinaryAttachment[A:Monoid](doc: Doc,
                                    attachmentName: String,
                                    sink: Channel[Task, Bytes, A],
                                    bufferSize: Int = 65536,
                                    params: Map[String, JsValue] = Map.empty): Couch[A] =
    liftF(GetBinary(this, doc, attachmentName, sink, Monoid[A], bufferSize, params, identity[A]))


  /**
   * Get the given attachment from the given document, writing it out directly to the file given in the path.
   * Provided as a nod to Dispatch optimization (the above form seems to make dispatch try to load the whole response
   * into memory first and I don't have time to track that down right now, so use the dispatch straight to file
   * convenience method for now).
   */
  def getBinaryAttachmentToFile(doc: Doc, attachmentName: String, filePath: String, params: Map[String, JsValue] = Map.empty): Couch[Boolean] =
    liftF(GetBinaryFile(this, doc, attachmentName, filePath, params, identity))

  /**
   * Download the given attachment from the given document, saving it to a file at the given path.
   */
  def downloadAttachment(doc: Doc, attachmentName: String, filePath: String, bufferSize: Int = 65536, params: Map[String, JsValue] = Map.empty): Couch[Unit] =
    getBinaryAttachmentToFile(doc, attachmentName, filePath, params = params).map(r => ())

  /**
   * Load the given attachment from the given document into memory, as a String.
   */
  def getAttachmentAsString(doc: Doc, attachmentName: String, params: Map[String, JsValue] = Map.empty): Couch[String] =
    getBinaryAttachment(doc, attachmentName, Process.eval(Task.now((b: Bytes) => Task.now(new String(b.take(b.size))))).repeat, params = params)

  /**
   * Attach the binary from the given input stream to the given document revision.
   */
  def attachBinary(doc: Option[Doc],
                   attachmentName: String,
                   contentLength: Long,
                   contentType: String,
                   rev: Option[Rev] = None,
                   body: File,
                   params: Map[String, JsValue] = Map.empty): Couch[Option[(Doc, Rev)]] =
    liftF(AttachBinary(this, doc, attachmentName, contentLength, contentType, rev, body, params, identity))

//  /**
//   * Attach the text from the given string to the given document revision.
//   */
//  def attachString(doc: Doc,
//                   attachmentName: String,
//                   body: String,
//                   rev: Option[Rev] = None
//                 ): Couch[Unit] = {
//    val bytes = body.getBytes("UTF-8")
//    attachBinary(doc, attachmentName, bytes.length, "text/plain", rev,
//      () => new ByteArrayInputStream(bytes))
//  }

  /**
   * Attach the given file to the given document revision.
   */
  def attachFile(doc: Option[Doc],
                 attachmentName: String,
                 file: File,
                 contentType: String,
                 rev: Option[Rev] = None,
                 bufferSize: Int = 4096,
                 params: Map[String, JsValue] = Map.empty,
                 es: java.util.concurrent.ExecutorService): Task[Couch[Option[(Doc, Rev)]]] = {
    Task { file.length }(es) map { sz =>
      attachBinary(doc, attachmentName, sz, contentType, rev, file, params = params)
    }
  }

  /**
   * Uploads the given file to a new empty document. Returns the document name and revision.
   */
  def uploadFile(file: File,
                 contentType: String,
                 bufferSize: Int = 4096,
                 params: Map[String, JsValue] = Map.empty)(attachmentName: String = file.getName, es: java.util.concurrent.ExecutorService): Task[Couch[(Doc, Rev)]] = {
    attachFile(None, attachmentName, file, contentType, None, bufferSize, params, es) map (_ map {
      case Some((doc, rev)) => doc -> rev
      case _ => sys.error(s"Document not found after we just created it :\\")
    })
  }

  // Convert a raw row to a tuple of optional ID to the document held in the given field
  def toRow(json: JsObject, field: String): (Option[String], JsValue) = {
    val obj = json.fields
    val key = obj.get("key") match {
      case Some(JsString(s)) => Some(s)
      case Some(JsNull) => None
      case Some(v) => obj.get("id") match {
        case Some(JsString(s)) => Some(s)
        case _ => sys.error(s"Expected id to be a string, got $v")
      }
      case None => sys.error(s"Row does not have a key field in $obj")
    }
    val value = obj.get(field) match {
      case Some(v) => v
      case None => sys.error(s"Row does not have a $field in $obj")
    }
    (key, value)
  }

}

object DB {
  import DefaultJsonProtocol._
  implicit val dbJsonFormat: JsonFormat[DB] = jsonFormat1(DB(_:String))
}

case class Doc(docID: String)
object Doc {
  import DefaultJsonProtocol._
  implicit val docJsonFormat: JsonFormat[Doc] = jsonFormat1(Doc(_:String))
}

case class Rev(revID: String)
object Rev {
  import DefaultJsonProtocol._
  implicit val revJsonFormat: JsonFormat[Rev] = jsonFormat1(Rev(_:String))
}

sealed trait CouchF[+A] {
  def map[B](f: A => B): CouchF[B]
}

case class CreateDB[+A](name: String, a: A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(a = f(a))
}

case class DropDB[+A](name: String, k: Boolean => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class GetAllDBs[+A](k: List[DB] => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class Fail[+A](e: Throwable) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] = Fail[B](e)
}

case class NewDoc[+A](db: DB,
                           doc: Option[Doc],
                           json: JsValue,
                           params: Map[String, JsValue],
                           k: (String \/ (Doc, Rev, Boolean)) => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class UpdateDoc[+A](db: DB,
                         doc: Doc,
                         rev: Rev,
                         json: JsValue,
                         params: Map[String, JsValue],
                         k: Option[(Doc, Rev)] => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class BulkUpdateDocs[+A](db: DB,
                              jsons: Seq[JsValue],
                              params: Map[String, JsValue],
                              k: Option[List[BulkOperationFailure \/ BulkOperationSuccess]] => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class DeleteDoc[+A](db: DB,
                         doc: Doc,
                         rev: Rev,
                         params: Map[String, JsValue],
                         k: Boolean => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class GetDoc[+A](db: DB,
                      doc: Doc,
                      params: Map[String, JsValue],
                      k: Option[(Doc, Rev, JsValue)] => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class GetAllDocsRaw[+A](db: DB,
                          params: Map[String, JsValue],
                          body: Option[JsValue],
                          k: List[JsValue] => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class GetAllDocIDs[+A](db: DB,
                            params: Map[String, JsValue],
                            k: List[Doc] => A,
                            includeDesignDocs: Boolean) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class QueryViewKeys[+A](db: DB,
                             design: Doc,
                             view: Doc,
                             params: Map[String, JsValue],
                             k: List[Doc] => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class QueryViewObjects[+A](db: DB,
                              design: Doc,
                              view: Doc,
                              params: Map[String, JsValue],
                              k: List[JsObject] => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

case class GetBinary[A,B](db: DB,
                          doc: Doc,
                          attachment: String,
                          channel: Channel[Task, Bytes, B],
                          m: Monoid[B],
                          bufferSize: Int,
                          params: Map[String, JsValue],
                          k: B => A) extends CouchF[A] {
  type T = B
  def map[C](f: A => C): CouchF[C] =
    copy(k = k andThen f)
}

case class GetBinaryFile[+A](db: DB,
                             doc: Doc,
                             attachment: String,
                             filePath: String,
                             params: Map[String, JsValue],
                             k: Boolean => A) extends CouchF[A] {

  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

// TODO: Model the body as a possibly asynchronously arriving stream of bytes
case class AttachBinary[A](db: DB,
                           doc: Option[Doc],
                           attachment: String,
                           contentLength: Long,
                           contentType: String,
                           rev: Option[Rev],
                           body: File,
                           params: Map[String, JsValue],
                           k: Option[(Doc, Rev)] => A) extends CouchF[A] {
  def map[B](f: A => B): CouchF[B] =
    copy(k = k andThen f)
}

object CouchF {
  implicit val couchFunctor: Functor[CouchF] = new Functor[CouchF] {
    def map[A,B](fa: CouchF[A])(f: A => B): CouchF[B] = fa map f
  }
}

