package stoop

import org.scalatest._
import prop.Checkers
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import java.util.concurrent._
import java.util.UUID

import spray.json._
import DefaultJsonProtocol._

import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent._
import scala.Some
import scalaz.\/-
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class CouchHttpTest extends FunSpec with ShouldMatchers with Checkers {

  implicit val execContext: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  val timeout = Duration(10, TimeUnit.SECONDS)
  val couch = CouchHttp(dbhost = "localhost", port = 5984, auth = None)
  it("uses my own execution context") {
    couch.e should be( execContext)
  }

  def addIdAndRev[A:JsonWriter](a: A, id: Doc, rev: Rev) =
    a.toJson.asJsObject.fields     +
     ("_id" -> JsString(id.docID)) +
     ("_rev" -> JsString(rev.revID))

  def newDBName = "testdb_" + UUID.randomUUID().toString

  def testWithDBTask[A](f: DB => Couch[A]): Task[A] = {
    val dbName = newDBName
    couch(for {
      _ <- createDB(dbName)
      a <- f(DB(dbName))
      _ <- dropDB(dbName)
    } yield a)
  }

  def testWithDB[A](f: DB => Couch[A]): A = testWithDBTask(f).run

  describe ("The CouchHttp client") {
    describe ("adding a new database") {
      val dbName = "testdb_" + UUID.randomUUID().toString
      it ("should not exist before adding, exist after adding, and not exist after deleting") {
        val test = couch {
          getAllDBs { dbs =>
            dbs.map(_.name).toList should not contain (dbName)

            // now we want to create it
            createDB(dbName) { newDb =>
              getAllDBs { dbs =>
                dbs.map(_.name) should contain (dbName)
                dropDB(dbName) { removedDb =>
                  getAllDBs { dbs =>
                    dbs.map(_.name).toList should not contain (dbName)
                    commit
                  }
                }
              }
            }

          }
        }
        test.run
      }
    }

    describe ("creating a database then dropping it") {
      val dbName = "testdb_" + UUID.randomUUID().toString
      it ("should return true since the database exists") {
        val test = couch {
          createDB(dbName) >> dropDB(dbName)
        }
        test.run should be (true)
      }
    }

    case class Person(name: String, age: Int)
    implicit val personFormat = jsonFormat2(Person)
    val alice   = Person("Alice", 21)
    val bob     = Person("Bob", 65)
    val charlie = Person("Charlie", 30)

    def fixture(db: DB) = for {
      _ <- db.newNamedDoc(Doc("alice"), alice)
      _ <- db.newNamedDoc(Doc("bob"), bob)
      _ <- db.newView("everything", Seq(
             CouchMapReduce(name = "all",
                            map = "function(doc) { emit(doc.name, doc.age) }",
                            reduce = "function(keys, values) { return sum(values) }")))
    } yield ()

    describe ("Adding named documents") {
      it ("The new documents should contain what we added") {
        testWithDB { db =>
          db.newNamedDoc(Doc("alice"), alice) >>
          db.newNamedDoc(Doc("bob"), bob) >>
          (for {
            Some((id1, rev1, p1)) <- db.getDoc[Person](Doc("alice"))
            Some((id2, rev2, p2)) <- db.getDoc[Person](Doc("bob"))
          } yield p1 == alice && p2 == bob)
        } should be (true)
      }
    }

    describe ("Listing all databases") {
      it ("should contain a database we just created") {
        testWithDB { db =>
          getAllDBs map (_ contains db)
        } should be (true)
      }
    }

    describe ("Updating a document") {
      it ("should create a new revision with the new content") {
        testWithDB { db => for {
          \/-(a)        <- db.newNamedDoc(Doc("alice"), alice)
          _               <- db.updateDoc(Doc("alice"), a._2, bob)
          Some((_, _, p)) <- db.getDoc[Person](Doc("alice"))
        } yield p } should be (bob)
      }
    }

    describe ("Bulk-updating documents") {
      it ("should actually update the docs and return a sequence of doc/rev pairs") {
        couch(createDB(newDBName) >>= { db => for {
          \/-(a) <- db.newNamedDoc(Doc("alice"), alice)
          \/-(b) <- db.newNamedDoc(Doc("bob"), bob)
          revs <- db.bulkUpdateDocs(List(addIdAndRev(alice.copy(age = 1), Doc("alice"), a._2),
                                         addIdAndRev(bob.copy(name = "Bill"), Doc("bob"), b._2)))
        } yield {
          revs.toList.flatMap(_.flatMap(_.toOption.map(_.id.get)))
        }
        }).run should be (List((Doc("alice")), Doc("bob")))
      }

    }

    describe ("Deleting a document") {
      it ("should fail gracefully when the document doesn't exist") {
        testWithDB { db => for {
          b1 <- db.deleteDoc(Doc("foo"), Rev("1"))
          b2 <- db.forceDeleteDoc(Doc("foo"))
        } yield b1 || b2 } should be (false)
      }
    }

    describe ("A new document") {
      it ("should be returned when we get it back") {
        testWithDB { db => for {
          (doc1, rev1) <- db.newDoc(alice)
          Some((doc2, rev2, a)) <- db.getDoc[Person](doc1)
        } yield doc1 == doc2 && rev1 == rev2 } should be (true)
      }

      it ("should no longer exist when we delete it") {
        testWithDB { db => for {
          (doc1, rev1) <- db.newDoc(alice)
          b <- db.deleteDoc(doc1, rev1)
          x <- db.getDoc[Person](doc1)
        } yield (x, b) } should be (None -> true)
      }
    }

    describe ("Getting all docIDs") {
      it("should return all the docIDs") {
        testWithDB { db => for {
          _ <- fixture(db)
          ids <- db.getAllDocIDs()
        } yield ids.length } should be (2)
      }
    }

    describe ("Querying a view") {
      it ("should succeed with a map view") {
        testWithDB { db => for {
          _ <- fixture(db)
          docs <- db.queryView[Map[String, String]](
            Doc("everything"),
            Doc("all"),
            Map("reduce" -> JsBoolean(false)))
        } yield docs.size } should be (2)
      }
      it ("should succeed with a reduce view") {
        testWithDB { db => for {
          _ <- fixture(db)
          docs <- db.queryView[Int](Doc("everything"),
                                    Doc("all"),
                                    Map("reduce" -> JsBoolean(true)))
        } yield docs(None) } should be (bob.age + alice.age)
      }
    }

    describe ("Querying view keys") {
      it ("through an identity view should return a document we just added") {
        testWithDB { db => for {
          \/-(a) <- db.newNamedDoc(Doc("alice"), alice)
          _ <- db.newView("everything",
                          Seq(CouchMap("all", "function(doc) { emit(null, doc) }")))
          docs <- db.queryViewKeys(Doc("everything"), Doc("all"))
        } yield docs contains Doc("alice") } should be (true)
      }
    }

    describe ("Getting a binary attachment") {
      import java.io._
      it ("should succeed for an attachment we just added") {
        val content = "foo"
        val f = File.createTempFile("bin", "test")
        f.deleteOnExit()
        val writer = new PrintWriter(f)
        writer.print(content)
        writer.flush()
        writer.close()
        val bufSize: Int = 2
        val res = testWithDB { db =>
          for {
            p <- db.newDoc(alice)
            _ <- db.attachBinary(Some(p._1),         // The document
                                 "foo.txt",    // Attachment name
                                 content.length,
                                 "text/plain", // Content type
                                 Some(p._2),         // Revision
                                 f)
            s <- db.getBinaryAttachment(
              p._1,
              "foo.txt",
              Process.eval(Task.now((b: Bytes) =>
                Task.now(new String(b.toArray.take(b.size))))).repeat, bufSize)
          } yield s
        }
        res should be (content)
      }

      it ("should succeed for an attachment we just added when downloading as a file") {
        val content = "foo"
        val f = File.createTempFile("bin", "test")
        f.deleteOnExit()
        val writer = new PrintWriter(f)
        writer.print(content)
        writer.flush()
        writer.close()
        val testFile = File.createTempFile("testoutputattach", "txt")
        testFile.deleteOnExit()
        val res = testWithDB { db =>
          for {
            p <- db.newDoc(alice)
            _ <- db.attachBinary(
              Some(p._1),         // The document
              "foo.txt",    // Attachment name
              content.length,
              "text/plain", // Content type
              Some(p._2),         // Revision
              f)
            s <- db.getBinaryAttachmentToFile(
              p._1,
              "foo.txt",
              testFile.getAbsolutePath
            )
          } yield s
        }
        res should be (true)

        assert(scala.io.Source.fromFile(testFile).getLines().next().startsWith("foo"))
      }
    }

  }

  import java.net.URI
  describe("Retrying") {
    it("should stop when the error doesn't match") {
      import scala.util.Random
      couch.retry(error[Boolean](HttpError(500, "Internal Server Error", new URI(""))),
                  Stream.continually(0.seconds),
                  _ => Random.nextInt % 2 == 0).
                attempt.run.fold(_ => true, _ => false) should be (true)
    }

    it("should retry once for each given delay") {
      var c = 0
      couch.retry(error[Boolean](HttpError(500, "Internal Server Error", new URI(""))),
                  Stream(0.seconds, 0.seconds, 0.seconds),
                  _ => { c += 1; true }).attempt.run
      c should be (3)
    }
  }

  describe("Deep retrying") {
    it("should retry only the portion that failed") {
      val dbName = newDBName
      couch.retryEach(createDB(dbName) >>= { _ =>
                        error[Boolean](HttpError(500, "Internal Server Error", new URI("")))
                      },
                      Stream(0.seconds)).attempt.run.fold(e => e match {
                        case HttpError(500, _, _) => true
                        case _ => false
                      }, _ => false) should be (true)
      couch(dropDB(dbName)).run
    }
  }

  describe("Fetch all docs raw") {
    it("can fetch something it just put in") {
      val dbName = newDBName
      val insertMe = Map("Yes" -> "please", "No" -> "thank you")
      val allDocs = testWithDB { db =>
          db.newDoc(insertMe) >>= { _ =>
            db.getAllDocsRaw[JsValue](Map[String, JsValue]())
          }
        }

      allDocs.size should be (1)

    }
  }

  describe("the execution context") {
    var used = new AtomicReference[Boolean](false)
    it("gets all the way into the Http request") {
      val es = new ThreadPoolExecutor(0, 2, 10 /* keepAliveTime in */ , TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](), Executors.defaultThreadFactory())  {
        override def afterExecute(r: Runnable, t: Throwable) {
          used.set(true)
          super.afterExecute(r, t)
        }
      }
      implicit val ec = ExecutionContext.fromExecutor(es)

      val subject = CouchHttp(dbhost = "localhost", port = 5984, auth = None)(ec)

      val name = newDBName
      val t = subject(createDB(name) >>= { _ => dropDB(name)} )  // do something, anything

      t.attemptRun

      used.get() should be (true)

    }

  }
}
