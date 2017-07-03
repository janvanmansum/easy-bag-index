package nl.knaw.dans.easy.bagindex.service

import java.util.UUID

import nl.knaw.dans.easy.bagindex.{ BagIndexApp, BagIndexDatabaseFixture, BagStoreFixture, ConfigurationSupportFixture, TestSupportFixture }
import org.scalamock.scalatest.MockFactory
import org.scalatra.test.scalatest.ScalatraSuite

import scala.util.Success

class BagIndexServletSpec extends TestSupportFixture
  with BagIndexDatabaseFixture
  with BagStoreFixture
  with ConfigurationSupportFixture
  with ScalatraSuite
  with MockFactory {

  val app = new BagIndexApp with TestConfiguration {}
  val servlet = BagIndexServlet(app)

  addServlet(servlet, "/*")

  "get" should "signal that the service is running" in {
    get("/") {
      status shouldBe 200
      body shouldBe "EASY Bag Index running."
    }
  }

//  "get search" should "search for a doi and return the appropriate JSON String if the accept header is specified" in {
//    app.addFromBagStore(UUID.fromString("00000000-0000-0000-0000-000000000001")) shouldBe a[Success[_]]
//    app.addFromBagStore(UUID.fromString("00000000-0000-0000-0000-000000000002")) shouldBe a[Success[_]]
//    app.addFromBagStore(UUID.fromString("00000000-0000-0000-0000-000000000003")) shouldBe a[Success[_]]
//
//    val doi = "10.5072/dans-2xg-umq8"
//    get("/search", params = Seq("doi" -> doi), headers = Seq("Accept" -> "application/json")) {
//      status shouldBe 200
//      body shouldBe
//        """
//          |
//        """.stripMargin
//    }
//  }
}
