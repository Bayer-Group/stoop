package stoop.attachments

import org.scalatest.FunSpec
import stoop.{TestDoc, DBInfo, DbAdmin}
import java.util.UUID
import org.scalatest.matchers.ShouldMatchers
import scala.concurrent.Await
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import spray.json._
import stoop.DbAdmin

case class TestDocWithBody(body: String)
object TestDocWithBody {
  import DefaultJsonProtocol._
  implicit val jformat = jsonFormat1(TestDocWithBody.apply)
}



class AttachmentUploaderTest extends FunSpec with ShouldMatchers {

  val serverUrl = "http://localhost:5984"

  describe ("The attachment uploader") {
    it ("should handle multiple text attachments") {
      val newDBname = "testdb_" + UUID.randomUUID().toString
      val admin = DbAdmin(serverUrl)
      admin.createDb(newDBname) { dbInfo =>

        val uploader = AttachmentUploader("localhost", 5984, newDBname)

        val s1 = "this is 21 chars long"
        val s2 = "this is 20 chars lon"
        val a1 = new NewAttachment("foo.txt", "text/plain", s1.getBytes.length, s1.getBytes)
        val a2 = new NewAttachment("bar.txt", "text/plain", s2.getBytes.length, s2.getBytes)

        val responseFuture = uploader.newDocWithAttachments(TestDocWithBody("This is a body."), Seq(a1, a2))
        val response = Await.result(responseFuture, Duration(10, TimeUnit.SECONDS))

        admin.dropDb(newDBname) { _ should be (true) }
      }
    }
  }

}
