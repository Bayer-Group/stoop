package stoop

import scala.language.higherKinds

import dispatch._

import scalaz.concurrent.Task
import Task._
import scalaz.stream._
import scalaz.Free.{freeMonad => _, _}

import com.ning.http.client._
import spray.json._
import DefaultJsonProtocol._
import com.ning.http.client.Response
import java.net.{URI, URISyntaxException}
import scala.concurrent.duration._
import scalaz.\/
import java.io.File
import java.util.concurrent.ExecutionException
import java.io.IOException
import java.util.UUID._
import scalaz.\/-
import scalaz.-\/

trait CouchInterpreter {
  // The other ways to run (retrying etc) can be added here
  def apply[A](action: Couch[A]): Task[A]
}

case class HttpError(code: Int, reason: String, uri: URI) extends Exception {
  override def toString = s"HttpError($code, $reason, $uri)"
  override def getMessage = toString
}

sealed abstract class BulkOperationResult {
  def id: Option[Doc]
  def rev: Option[Rev]
}

case class BulkOperationSuccess(id: Option[Doc], rev: Option[Rev]) extends BulkOperationResult
case class BulkOperationFailure(id: Option[Doc], rev: Option[Rev], error: String, reason: String) extends BulkOperationResult

/** A CouchDB algebra that uses Dispatch for HTTP */
case class CouchHttp(dbhost: String, port: Int, auth: Option[(String, String)], timeout: Duration = 1.hour, idleTimeout: Duration = 1.hour)
                    (implicit val e: scala.concurrent.ExecutionContext) extends CouchInterpreter {
  import CouchHttp._

  import language.implicitConversions

  val limitedTimeHttp = Http.configure { h => h.
    setAllowPoolingConnection(true).
    setRequestTimeoutInMs(timeout.toMillis.toInt).
    setIdleConnectionTimeoutInMs(idleTimeout.toMillis.toInt)
  }

  implicit def fromDispatch[A](b: RequestBuilder)(a: => scala.concurrent.Future[A]): Task[A] =
    Task async { k => a.onComplete { t =>
      val result: Throwable \/ A = try {
        \/-(t.get)
      }
      catch {
        case cee: ExecutionException => cee.getCause match {
          // more couch pandering - NFEs and IOExceptions within the ning async handler are possible when
          // couch sends back nonsense. Treat as internal server error for retrying.
          case serverEx @ (_: NumberFormatException | _: IOException) =>
            val trace = try {
              serverEx.getStackTrace.take(40).map(_.toString).mkString("\n")
            } catch {
              case ex: Exception => "Stack trace failed"
            }
            -\/(HttpError(503, serverEx.getMessage + "\n" + trace, safeUriFromRequestBuilder(b)))
          case ex: Throwable => -\/(ex)
        }
        case ex: Throwable => -\/(ex)
      }
      k(result)}(e)
    }

  // Side-effect
  def ok[A](b: RequestBuilder) = req(b) flatMap { r => (r.getStatusCode / 100) match {
    case 2 => now(r.getResponseBody)
    case _ => fail(mkError(r))
  }}

  private def safeUriFromRequestBuilder(b: RequestBuilder): URI = try {
    new URI(b.build().getRawUrl)    // in a try, because we really don't want a failure here, skipping
    // the URI is preferable
  } catch {
    case _: URISyntaxException => new URI("")
  }

  private def incompleteRequestHandler(b: RequestBuilder, ex: Exception): Task[Nothing] = {
    // when couch sends back crap, it manifests in ning as a number format exception while trying to parse
    // the status code, count this as an internal server error (Service Unavailable) and pass on the info that way
    val trace = try {
      ex.getStackTrace.take(40).map(_.toString).mkString("\n")
    } catch {
      case ex: Exception => "Stack trace failed"
    }
    fail(HttpError(503, ex.getMessage + "\n" + trace, safeUriFromRequestBuilder(b)))
  }

  def req(b: RequestBuilder): Task[Response] = {
    try {
      fromDispatch(b)(limitedTimeHttp(b)(e)).flatMap { r =>
        (r.getStatusCode / 100) match {
          case 5 => fail(mkError(r))
          case _ => now(r)
        }
      }
    } catch {
      case nfe: java.lang.NumberFormatException => incompleteRequestHandler(b, nfe)
    }
  }

  case class DownloadResponseDetails(code: Int, reason: String, uri: URI)
  def reqToFile(b: RequestBuilder, f: File): Task[DownloadResponseDetails] = {
    try {
      val handler = new SimpleFileDownloader(f)
      fromDispatch(b)(limitedTimeHttp(b.build(), handler)(e)).flatMap {
        r => now(DownloadResponseDetails(handler.statusCode, handler.reason, f.toURI))
      }
    } catch {
      case nfe: java.lang.NumberFormatException => incompleteRequestHandler(b, nfe)
    }
  }

  /* For POC purposes ignore the auth passed in because encryption is hard */
  def base = auth match {
    case Some(credentials)=>host(dbhost).secure.as_!(credentials._1, credentials._2)
    case None => host(dbhost, port)
  }

  def addJson(b: RequestBuilder) = b.addHeader("Content-Type", "application/json")

  def responseObj(r: Response): Map[String, JsValue] =
    if (r.hasResponseBody) stringObj(r.getResponseBody) else Map()
  def stringObj(json: String): Map[String, JsValue] =
    JsonParser(json).asJsObject.fields
  def getRows(json: String): List[JsValue] =
    stringObj(json)("rows").convertTo[List[JsValue]]

  def defaultDelays = 1 to 12 map (_ => (scala.util.Random.nextInt(900) + 100).milliseconds)

  /**
   * Keep running the given action on http errors with codes matching the predicate.
   * The sequence of delays determines how long to keep retrying and how long to wait
   * between retries. E.g. Seq(1.second, 2.seconds) will retry twice; first after one
   * second and then again 2 seconds after that.
   * The default is to retry 12 times with a random interval between 100 and 1000 ms.
   * The default is to retry error codes 500 and above.
   */
  def retry[A](action: Couch[A],
               delays: Seq[Duration] = defaultDelays,
               p: Int => Boolean = _ >= 500): Task[A] =
    apply(action).retry(delays, {
      case HttpError(e, _, _) => p(e)
      case _ => false
    })

  /**
   * Turns the Couch script into a `Task` that executes it as an HTTP request.
   */
  def apply[A](action: Couch[A]): Task[A] = action.runM(step)

  /**
   * Runs the given action and retries any portion of it that fails with http errors matching the given predicate.
   * The sequence of delays determines how long to keep retrying and how long to wait
   * between retries. E.g. Seq(1.second, 2.seconds) will retry twice; first after one
   * second and then again 2 seconds after that.
   * The default is to retry 12 times with a random interval between 100 and 1000 ms.
   * The default is to retry error codes 500 and above.
   */
  def retryEach[A](action: Couch[A],
                   delays: Seq[Duration] = defaultDelays,
                   p: Int => Boolean = _ >= 500): Task[A] =
    action.runM(x => step(x).retry(delays, {
      case HttpError(e, _, _) if p(e) => true
    }))

  private def toListMap[K, V](docList: List[(K, V)]): Map[K, V] = {
    val b = collection.immutable.ListMap.newBuilder[K, V]
    for (x <- docList)
      b += x

    b.result
  }

  /**
   * Runs the given action until a falure matching the predicate is encountered.
   * If there was a failure, this method returns an action that picks up where we left off,
   * along with the error that was encountered. If there were no errors, it simply returns the answer.
   */
  def runUntilFailure[A](action: Couch[A],
                         p: Int => Boolean = _ >= 500): Task[(Couch[A], HttpError) \/ A] =
    action.resume match {
      case -\/(s) => step(s).attempt flatMap {
        case -\/(e@HttpError(_, _, _)) => Task.now(\/.left(action -> e))
        case -\/(e) => Task.fail(e)
        case \/-(a) => runUntilFailure(a, p)
      }
      case \/-(a) => Task.now(\/.right(a))
    }

  /**
   * Run a single step of the given couch action.
   */
  def step[A](action: CouchF[Couch[A]]): Task[Couch[A]] = action match {
    case Fail(e) => fail(e)
    case CreateDB(name, a) =>
      ok((base / name).PUT) map (_ => a)
    case DropDB(name, k) =>
      req((base / name).DELETE).map { r => k(r.getStatusCode != 404) }
    case GetAllDBs(k) =>
      ok((base / "_all_dbs").GET) map (s =>
        k(JsonParser(s).convertTo[List[String]].map(DB(_))))
    case NewDoc(db, optDoc, json, params, k) =>
      val args = params.mapValues(_.toString)
      val doc = optDoc getOrElse freshName
      val urlParts = doc.docID.split("/").foldLeft(base / db.name)(_ / _)
      req(urlParts.PUT.setBody(json.toString) <<? args) flatMap { r =>
        r.getStatusCode match {
          case code@(201|202) =>
            val JsString(rev) = responseObj(r)("rev")
            delay(k(\/.right((doc, Rev(rev), code == 202))))
          case 409 =>
            val JsString(reason) = responseObj(r)("reason")
            delay(k(\/.left(reason)))
          case _ => fail(mkError(r))
        }
      }
    case UpdateDoc(db, doc, rev, json, params, k) =>
      val args = params.mapValues(_.toString)
      val obj = (json.asJsObject.fields + ("_id" -> doc.docID.toJson) + ("_rev" -> rev.revID.toJson)).toJson
      req((base / db.name / doc.docID).PUT.setBody(obj.toString) <<? args) flatMap { r =>
        r.getStatusCode match {
          case 201 =>
            val JsString(newRev) = responseObj(r)("rev")
            delay(k(Some(doc -> Rev(newRev))))
          case 409 => delay(k(None))
          case _   => fail(new Exception("Document update error", mkError(r)))
        }
      }
    case BulkUpdateDocs(db, jsons, params, k) =>
      val args = params.mapValues(_.toString)
      req(addJson((base / db.name / "_bulk_docs").POST <<? args).setBody(Map("docs" -> jsons).toJson.toString)) flatMap { r =>
        r.getStatusCode match {
          case code @ (201|202) =>
            val rows = JsonParser(r.getResponseBody).convertTo[List[JsValue]]
            delay(k(Some(rows.map(v => {
              val obj = v.asJsObject.fields
               obj.get("error") match {
                case (Some(jsError)) =>
                  \/.left(BulkOperationFailure(
                    obj.get("id").map(id => Doc(id.convertTo[String])),
                    obj.get("rev").map(rev => Rev(rev.convertTo[String])),
                    jsError.convertTo[String],
                    obj.get("reason").map(_.convertTo[String]).getOrElse("")
                  ))
                case _ =>
                  \/.right(BulkOperationSuccess(
                    obj.get("id").map(id => Doc(id.convertTo[String])),
                    obj.get("rev").map(rev => Rev(rev.convertTo[String]))
                  ))
              }
            }))))
          case 409 => delay(k(None))
          case _   => fail(new Exception("Document bulk update error", mkError(r)))
        }
      }
    case DeleteDoc(db, doc, rev, params, k) =>
      val args = params.mapValues(_.toString)
      ok((base / db.name / doc.docID).DELETE.
        addQueryParameter("rev", rev.revID) <<? args).attempt map (_.fold(_ => k(false), _ => k(true)))
    case GetDoc(db, doc, params, k) =>
      val args = params.mapValues(_.toString)
      val urlParts = doc.docID.split("/").foldLeft(base / db.name)(_ / _)
      req(urlParts.GET <<? args) flatMap { r =>
        r.getStatusCode match {
          case 200 =>
            val obj = responseObj(r)
            val JsString(id) = obj("_id")
            val JsString(rev) = obj("_rev")
            delay(k(Some((Doc(id), Rev(rev), obj.toJson))))
          case 404 => delay(k(None))
          case _ => fail(mkError(r))
        }
      }
    case GetAllDocIDs(db, params, k, includeDesignDocs) =>
      def handleRow(row: JsObject) = row.fields.get("key") match {
        case Some(JsString(s)) => if (!includeDesignDocs && (s startsWith "_")) None else Some(Doc(s))
        case Some(_) => sys.error(s"Key is not a string in row $row")
        case None => sys.error(s"No key in row $row")
      }
      val args = params.mapValues(_.toString)
      ok((base / db.name / "_all_docs").GET <<? args) map (r => {
        k(getRows(r).flatMap(row => handleRow(row.asJsObject)))
      })
    case GetAllDocsRaw(db, params, body, k) =>
      val args = params.mapValues(_.toString)
      val url = base / db.name / "_all_docs"
      val results = if (body.isEmpty) { ok(url.GET <<? args) } else { ok((url.POST <<? args).setBody(body.get.toString)) }
      results map { r =>
        val rowsAsJson = getRows(r)
        k(rowsAsJson)
      }
    case QueryViewKeys(db, design, view, params, k) =>
      val args = params.mapValues(_.toString)
      val url  = base / db.name / "_design" / design.docID / "_view" / view.docID
      ok(url.GET <<? args) map { r =>
        stringObj(r).get("rows") match {
          case Some(rs) => k(rs.convertTo[List[JsValue]] map {
            case JsObject(obj) => obj.get("id") match {
              case Some(JsString(s)) => Doc(s)
              case _ => throw new Exception("Nothing found satisfying the query")
            }
            case v => throw new Exception(s"Expected an object instead of $v")
          })
          case None => throw new Exception("Expected \"rows\" to exist when querying a view")
        }
      }
    case QueryViewObjects(db, design, view, params, k) =>
      val args = params.mapValues(_.toString)
      val url = base / db.name / "_design" / design.docID / "_view" / view.docID
      ok(url.GET <<? args) map { r =>
        val jsonRows: List[JsObject] = getRows(r).map(_.asJsObject)
        k(jsonRows)
      }
    case gb@GetBinary(db, doc, attachment, channel, monoid, bufSize, params, k) =>
      val args = params.mapValues(_.toString)
      req((base / db.name / doc.docID / attachment).GET <<? args) flatMap { r =>
        (r.getStatusCode / 100) match {
          case 2 =>
            val buf     = new Array[Byte](bufSize)
            val buffer  = Process.eval(Task.now(buf)).repeat
            val chunker = io.unsafeChunkR(r.getResponseBodyAsStream)
            val bytes   = buffer through chunker
            val stream  = bytes through channel
            stream.fold(monoid.zero)((a, b) => monoid.append(a, b)).runLast map (o => k(o.get))
          case _ => fail(new Exception(s"Error retrieving attachment $attachment from doc ${doc.docID}", mkError(r)))
        }
      }
    case gbf@GetBinaryFile(db, doc, attachment, filePath, params, k) =>
      val args = params.mapValues(_.toString)
      reqToFile((base / db.name / doc.docID / attachment).GET <<? args, new File(filePath)) flatMap { downloadResponse =>
        (downloadResponse.code / 100) match {
          case 2 =>
            delay(k(true))
          case _ => fail(new Exception(s"Error retrieving attachment $attachment from doc ${doc.docID}",
            HttpError(downloadResponse.code, downloadResponse.reason, downloadResponse.uri)))
        }
      }
    case AttachBinary(db, optDoc, attachment, length, contentType, rev, file, params, k) =>
      val args = params.mapValues(_.toString)
      val doc = optDoc getOrElse freshName
      val areq = (base / db.name / doc.docID / attachment).PUT <<? args
      val areq2 = rev.map(x => areq.addQueryParameter("rev", x.revID)).getOrElse(areq).
                addHeader("Content-Type", contentType).
                setBody(file)
      req(areq2) flatMap { r =>
        r.getStatusCode match {
          case 200|201 =>
            val obj = responseObj(r)
            val JsBoolean(ok) = obj("ok")
            if (ok) {
              val JsString(id) = obj("id")
              val JsString(rev) = obj("rev")
              delay(k(Some((Doc(id), Rev(rev)))))
            } else {
              delay(k(None))
            }
          case _ => fail(mkError(r))
        }
      }
  }
}

object CouchHttp {
  def mkError(r: Response) =
    HttpError(r.getStatusCode, r.getStatusText, r.getUri)

  import scalaz.Show
  import scalaz.Cord
  implicit def responseShow: Show[Response] = new Show[Response] {
    import scalaz.Scalaz._
    import scala.collection.JavaConversions
    override def show(r: Response) =
      Cord(r.getStatusCode.toString, " ", r.getStatusText, "\n",
           JavaConversions.mapAsScalaMap(r.getHeaders).mapValues(
             JavaConversions.collectionAsScalaIterable(_).toList).toMap.show)
  }

  private def freshName = Doc(randomUUID().toString.replaceAll("-", ""))
}

