package stoop.attachments

import dispatch._
import spray.json._
import com.ning.http.multipart.Part
import java.io.OutputStream

/**
 * Attachment handling for Stoop
 */
case class Attachment(attachName: String, contentType: String, length: Long)
object Attachment {
  import DefaultJsonProtocol._
  implicit val attachJsonFormat = new JsonFormat[Attachment] {
    def write(obj: Attachment): JsValue =
      JsObject(Map(obj.attachName -> JsObject(Map(
        "follows" -> JsBoolean(true),
        "content_type" -> JsString(obj.contentType),
        "length" -> JsNumber(obj.length)
      ))))

    def read(doc: JsValue): Attachment = try {
      doc match {
        case JsObject(fields) if fields.size == 1 =>
          val name = fields.keys.head
          val attachFields = fields.values.head.asJsObject.fields
          val follows = attachFields("follows").convertTo[Boolean]
          val contentType = attachFields("content_type").convertTo[String]
          val length = attachFields("length").convertTo[Long]
          Attachment(name, contentType, length)
      }
    }
    catch {
      case ex: Exception => throw new IllegalStateException(s"Bad attachment JSON format: $doc")
    }
  }
}

class NewAttachment(override val attachName: String, override val contentType: String, override val length: Long, dataFn: => Array[Byte])
    extends Attachment(attachName, contentType, length) {

  def data: Array[Byte] = dataFn
}
object NewAttachment {
  import DefaultJsonProtocol._
  implicit val newAttachJsonWriter = new JsonWriter[NewAttachment] {
    def write(obj: NewAttachment): JsValue = Attachment.attachJsonFormat.write(obj)
  }
}

case class MultiPartAttachmentAdapter(attachment: NewAttachment) extends Part {
  def getName: String = attachment.attachName
  def getContentType: String = attachment.contentType
  def getCharSet: String = null
  def getTransferEncoding: String = null
  def sendData(out: OutputStream) { out.write(attachment.data) }
  def lengthOfData: Long = attachment.length
}
