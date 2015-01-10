package stoop

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import java.util.UUID
import spray.json.DefaultJsonProtocol

/**
 * Created with IntelliJ IDEA.
 * User: rlwall2
 * Date: 5/8/13
 * Time: 8:36 AM
 */


case class TestDoc(_id: String, _rev: String, name: String, age: Int, rating: Long) extends HasId
object TestDoc extends DefaultJsonProtocol {
  implicit val testDocFormat = jsonFormat5(TestDoc.apply)
}

import TestDoc._

class FlowTest extends FunSpec with ShouldMatchers {

  val serverUrl = "http://localhost:5984"

  describe ("The Flow client") {
    describe ("given a GetAllDBs request") {
      it ("should list the DBs") {
        val admin = DbAdmin(serverUrl)
        admin.listDbs { dbList =>
          dbList.head.dbName should not be ('empty)
          dbList.head.server should be (serverUrl)
        }
      }
    }

    describe ("adding a new database") {
      val newDBname = "testdb_" + UUID.randomUUID().toString
      it ("should not exist before adding, exist after adding, and not exist after deleting") {
        val admin = DbAdmin(serverUrl)

        admin.listDbs { dbList =>
          dbList.contains(DBInfo(serverUrl, newDBname)) should be (false)
        }

        admin.createDb(newDBname) { dbInfo =>
          dbInfo.server should be (serverUrl)
          dbInfo.dbName should be (newDBname)

          admin.listDbs { dbList =>
            dbList.contains(DBInfo(serverUrl, newDBname)) should be (true)
          }
        }

        admin.dropDb(newDBname) { worked =>
          worked should be (true)
          admin.listDbs { dbList =>
            dbList.contains(DBInfo(serverUrl, newDBname)) should be (false)
          }
        }
      }
    }
  }

  describe("The flow DBInfo client") {
    describe("for a new DBInfo") {

      val admin = DbAdmin(serverUrl)
      val testDbName = "testdbinfo_" + UUID.randomUUID().toString
      val db = admin.createDb(testDbName) { dbInfo => dbInfo }
      val someId = UUID.randomUUID().toString

      it ("should allow documents to be created") {
        db.get[TestDoc](someId) { obj =>
          obj should be (None)
        }

        // now create one
        val testDoc = TestDoc(someId, "", "hello", 12, 1L)
        db.create(testDoc) { newDoc =>
          newDoc should not be (None)
          newDoc._id should be (someId)
          newDoc._rev should not be ('empty)
          newDoc.name should be ("hello")
          newDoc.age should be (12)
          newDoc.rating should be (1.0)

          db.get[TestDoc](someId) { found =>
            found should not be (None)
            found should be (Some(newDoc))
          }
        }
      }
    }
  }
}

