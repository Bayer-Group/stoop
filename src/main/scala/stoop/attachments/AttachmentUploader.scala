package stoop.attachments

import dispatch._
import spray.json._
import com.ning.http.multipart.Part
import com.ning.http.client.RequestBuilder
import scala.collection.mutable.ArrayBuffer
import stoop.FieldTweaker
import java.util.UUID

/**
 * Created with IntelliJ IDEA.
 * User: rlwall2
 * Date: 5/14/13
 * Time: 8:34 AM
 */

// TODO: use another execution context?
import scala.concurrent.ExecutionContext.Implicits.global

case class AttachmentUploader(dbServer: String, port: Int, dbName: String) extends FieldTweaker {
  val base = host(dbServer, port)

  def newDocWithAttachments[A : JsonFormat](doc: A, attachments: Seq[NewAttachment]) = {
    val docAsJson = doc.toJson
    val jsObj = docAsJson.asJsObject("Must be a JsObject to have attachments")
    val withAttachments = jsObj.fields ++ Map("_attachments" ->
      JsObject(attachments.foldLeft(Map.empty[String, JsValue])(_ ++ _.toJson.asJsObject.fields))
    )
    // now we need multi-part mime encoding for the attachment data
    val request = (base / dbName / ("multipart" + UUID.randomUUID().toString)).PUT

    MultipartSupport.build(request, fixFieldsForWrite(JsObject(withAttachments)).prettyPrint, attachments)
  }

}


object MultipartSupport {
  private val CRLF = "\r\n"
  private val PREF = "--"
  private val CHARSET = "UTF-8"
  private val BOUNDARY = "abc123"  // TODO: change this
  private val PART_SEPARATOR = (PREF + BOUNDARY + CRLF).getBytes(CHARSET)

  def build(bb: RequestBuilder, docJson: String, attachments: Seq[NewAttachment]) = {
    val body = new ArrayBuffer[Byte]
    body ++= PART_SEPARATOR
    body ++= ("content-type: application/json" + CRLF * 2).getBytes(CHARSET)
    body ++= docJson.getBytes(CHARSET)
    body ++= (CRLF * 2).getBytes(CHARSET)

    attachments.foreach { attach =>
      body ++= PART_SEPARATOR
      body ++= CRLF.getBytes(CHARSET)
      body ++= attach.data
      body ++= CRLF.getBytes(CHARSET)
    }

    body ++= PART_SEPARATOR

    val builder = bb
      .addHeader("MIME-version", "1.0")
      .addHeader("Content-Type", "multipart/related;boundary="+BOUNDARY)
      .setBody(body.toArray)
    val req = Http(builder OK as.String).either.map(scalaz.\/.fromEither)
    req
  }
}

