package stoop

import com.ning.http.client.{HttpResponseBodyPart, HttpResponseStatus, HttpResponseHeaders, AsyncHandler}
import java.io.{FileOutputStream, File}
import com.ning.http.client.AsyncHandler.STATE

/**
 * Created with IntelliJ IDEA.
 * User: rlwall2
 * Date: 4/22/14
 * Time: 4:32 PM
 * Use ning's async handler to download arbitrarily large files for attachments
 */
class SimpleFileDownloader(f: File) extends AsyncHandler[File] {
  private val out = new FileOutputStream(f)
  private var failed: Boolean = false
  private var reasonCode: Int = 200
  private var reasonString: String = ""

  def statusCode: Int = reasonCode
  def reason: String = reasonString

  def onThrowable(t: Throwable) {
    failed = true
  }

  def onBodyPartReceived(bodyPart: HttpResponseBodyPart): STATE = {
    if (!failed) bodyPart.writeTo(out)
    STATE.CONTINUE
  }

  def onStatusReceived(responseStatus: HttpResponseStatus): STATE = {
    if (responseStatus.getStatusCode != 200) {
      failed = true
      reasonCode = responseStatus.getStatusCode
      reasonString = responseStatus.getStatusText
    }
    if (failed) STATE.ABORT else STATE.CONTINUE
  }

  def onHeadersReceived(headers: HttpResponseHeaders): STATE = STATE.CONTINUE

  def onCompleted(): File = {
    out.close()
    if (failed) {
      f.delete()
      null    // yuk - but we are in Java land here
    }
    else f
  }
}
