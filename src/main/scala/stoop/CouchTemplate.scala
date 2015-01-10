package stoop

import java.net.URLEncoder

sealed trait CouchTemplate {
  protected def paramsStr(params: Map[String, String], startChar: String = "?"): String =
    if (params.isEmpty) "" else (for {
      (name, value) <- params
    } yield s"$name=${URLEncoder.encode(value, "UTF-8")}").mkString(startChar, "&", "")

}

case class CouchServerTemplate(dbServer: String) extends CouchTemplate {
  lazy val serverURL = dbServer

  def forDBName(dbName: String, options: Map[String, String] = Map.empty): String =
    Seq(serverURL, dbName).mkString("/") + paramsStr(options)

  def listDBs = s"$serverURL/_all_dbs"

  def listDBs(options: Map[String, String]): String =
    listDBs + paramsStr(options)
}

case class CouchDBTemplate(dbServer: String, dbName: String) extends CouchTemplate {

  lazy val dbURL = Seq(dbServer, dbName).mkString("/")

  def forId(id: String, options: Map[String, String] = Map.empty): String = {
    s"$dbURL/$id${paramsStr(options)}"
  }

  lazy val forPost: String = s"$dbURL"

  lazy val forBulkDocs: String = s"$dbURL/_bulk_docs"

  def forIdRev(id: String, rev: String, options: Map[String, String] = Map.empty): String = {
    s"$dbURL/$id?rev=$rev${paramsStr(options, "&")}"
  }

  def forAllDocs: String = s"$dbURL/_all_docs"

  def forAllDocs(options: Map[String, String] = Map.empty): String = {
    s"$dbURL/_all_docs${paramsStr(options)}"
  }

}
