/**
 * Copyright (C) 2017 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.bagindex.server

import java.util.{ TimeZone, UUID }

import nl.knaw.dans.easy.bagindex.components.{ DatabaseComponent, IndexBagComponent }
import nl.knaw.dans.easy.bagindex._
import org.joda.time.{ DateTime, DateTimeZone }
import org.scalamock.scalatest.MockFactory
import org.scalatra.test.scalatest.ScalatraSuite

import scala.util.Success
import scala.xml.XML

class BagIndexServletSpec extends TestSupportFixture
  with BagIndexDatabaseFixture
  with BagStoreFixture
  with Bagit5Fixture
  with ConfigurationSupportFixture
  with ServletFixture
  with ScalatraSuite
  with MockFactory
  with CustomMatchers
  with BagIndexServletComponent
  with IndexBagComponent
  with DatabaseComponent {

  override val database: Database = new Database {}
  override val index: IndexBag = new IndexBag {}
  override val bagIndexServlet = new BagIndexServlet {}

  override def beforeAll(): Unit = {
    super.beforeAll()
    DateTimeZone.setDefault(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Europe/Amsterdam")))
    addServlet(bagIndexServlet, "/*")
  }

  "get" should "signal that the service is running" in {
    get("/") {
      status shouldBe 200
      body shouldBe "EASY Bag Index running."
    }
  }

  "get search" should "search for a doi and return the appropriate JSON String if the accept header is specified" in {
    val uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val uuid3 = UUID.fromString("00000000-0000-0000-0000-000000000003")
    index.addFromBagStore(uuid1) shouldBe a[Success[_]]
    index.addFromBagStore(uuid2) shouldBe a[Success[_]]
    index.addFromBagStore(uuid3) shouldBe a[Success[_]]

    val doi = "10.5072/dans-2xg-umq8"
    val created = DateTime.parse("2017-01-16T14:35:00.888")
    get("/search", params = Seq("doi" -> doi), headers = Seq("Accept" -> "application/json")) {
      status shouldBe 200
      body shouldBe
        s"""{
          |  "result":[{
          |    "bag-info":{
          |      "bag-id":"$uuid1",
          |      "base-id":"$uuid1",
          |      "created":"${created.toString(dateTimeFormatter)}",
          |      "doi":"$doi"
          |    }
          |  }]
          |}""".stripMargin
    }
  }

  it should "search for a doi and return the appropriate XML if the accept header is specified" in {
    val uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val uuid3 = UUID.fromString("00000000-0000-0000-0000-000000000003")
    index.addFromBagStore(uuid1) shouldBe a[Success[_]]
    index.addFromBagStore(uuid2) shouldBe a[Success[_]]
    index.addFromBagStore(uuid3) shouldBe a[Success[_]]

    val doi = "10.5072/dans-2xg-umq0"
    val created = DateTime.parse("2017-01-18T14:35:00.888")
    get("/search", params = Seq("doi" -> doi), headers = Seq("Accept" -> "text/xml")) {
      status shouldBe 200
      XML.loadString(body) should equalTrimmed {
        <result>
          <bag-info>
            <bag-id>{uuid3}</bag-id>
            <base-id>{uuid1}</base-id>
            <created>{created.toString(dateTimeFormatter)}</created>
            <doi>{doi}</doi>
          </bag-info>
        </result>
      }
    }
  }

  it should "return an empty result when the doi parameter is not found" in {
    val doi = "10.5072/dans-2xg-umq8"
    get("/search", params = Seq("doi" -> doi), headers = Seq("Accept" -> "application/json")) {
      status shouldBe 200
      body shouldBe
        s"""{
           |  "result":[]
           |}""".stripMargin
    }
  }

  it should "fail with a BadRequest when searching for something unknown" in {
    get("/search", "unknown-field" -> "abc") {
      status shouldBe 400
      body shouldBe "query parameter not supported"
    }
  }

  it should "fail with a BadRequest when no parameter is specified" in {
    get("/search") {
      status shouldBe 400
      body shouldBe "no search query specified"
    }
  }

  "get bag-sequence" should "return a list of all bags in a sequence, given one of the bagIds" in {
    val uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val uuid3 = UUID.fromString("00000000-0000-0000-0000-000000000003")
    index.addFromBagStore(uuid1) shouldBe a[Success[_]]
    index.addFromBagStore(uuid2) shouldBe a[Success[_]]
    index.addFromBagStore(uuid3) shouldBe a[Success[_]]

    get("/bag-sequence", "contains" -> uuid2.toString) {
      status shouldBe 200
      body shouldBe s"$uuid1\n$uuid2\n$uuid3"
    }
  }

  it should "return an empty result when the bagId is unknown" in {
    get("/bag-sequence", "contains" -> UUID.randomUUID().toString) {
      status shouldBe 200
      body shouldBe empty
    }
  }

  it should "fail when the parameter 'contains' is not present" in {
    get("/bag-sequence") {
      status shouldBe 400
      body shouldBe "query parameter 'contains' not found"
    }
  }

  it should "fail when the parameter 'contains' is not a well-formatted UUID" in {
    get("/bag-sequence", "contains" -> "abc-def-ghi-jkl-mno") {
      status shouldBe 400
      body shouldBe "invalid UUID string: abc-def-ghi-jkl-mno"
    }
  }

  it should "fail when the parameter 'contains' is not a UUID" in {
    get("/bag-sequence", "contains" -> "foobar") {
      status shouldBe 400
      body shouldBe "invalid UUID string: foobar"
    }
  }

  it should "fail when the parameter is unknown" in {
    get("/bag-sequence", "unknown-parameter" -> UUID.randomUUID().toString) {
      status shouldBe 400
      body shouldBe "query parameter 'contains' not found"
    }
  }

  "get bags" should "return the info of a bag, given its bagId" in {
    val uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val uuid3 = UUID.fromString("00000000-0000-0000-0000-000000000003")
    index.addFromBagStore(uuid1) shouldBe a[Success[_]]
    index.addFromBagStore(uuid2) shouldBe a[Success[_]]
    index.addFromBagStore(uuid3) shouldBe a[Success[_]]

    val doi = "10.5072/dans-2xg-umq0"
    val created = DateTime.parse("2017-01-18T14:35:00.888")
    get(s"/bags/$uuid3", headers = Seq("Accept" -> "application/xml")) {
      status shouldBe 200
      XML.loadString(body) should equalTrimmed {
        <result>
          <bag-info>
            <bag-id>{uuid3}</bag-id>
            <base-id>{uuid1}</base-id>
            <created>{created.toString(dateTimeFormatter)}</created>
            <doi>{doi}</doi>
          </bag-info>
        </result>
      }
    }
  }

  it should "return an empty response when the bagId is unknown" in {
    get(s"/bags/${UUID.randomUUID()}") {
      status shouldBe 200
      body shouldBe
        s"""{
           |  "result":[]
           |}""".stripMargin
    }
  }

  it should "fail when the parameter 'bagId' is not present" in {
    get("/bags") {
      status shouldBe 404
    }
  }

  it should "fail when the parameter 'bagId' is not a well-formatted UUID" in {
    get("/bags/abc-def-ghi-jkl-mno") {
      status shouldBe 400
      body shouldBe "invalid UUID string: abc-def-ghi-jkl-mno"
    }
  }

  it should "fail when the parameter 'bagId' is not a UUID" in {
    get("/bags/foobar") {
      status shouldBe 400
      body shouldBe "invalid UUID string: foobar"
    }
  }

  "put bags" should "insert a bag from the bagstore into the database, given its bagId" in {
    val uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000002")
    index.addFromBagStore(uuid1) shouldBe a[Success[_]]

    put(s"/bags/$uuid2") {
      status shouldBe 201
      body shouldBe empty
      database.getBagInfo(uuid2) should matchPattern { case Success(BagInfo(`uuid2`, `uuid1`, _, _)) => }
    }
  }

  it should "fail when the bag is already in the database" in {
    val uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001")
    index.addFromBagStore(uuid1) shouldBe a[Success[_]]

    put(s"/bags/$uuid1") {
      status shouldBe 400
      body shouldBe s"Bag '$uuid1' is already in the index"
      database.getBagInfo(uuid1) should matchPattern { case Success(BagInfo(`uuid1`, `uuid1`, _, _)) => }
    }
  }

  it should "fail when the bag is not found in the bagstore" in {
    val uuid = UUID.randomUUID()

    put(s"/bags/$uuid") {
      status shouldBe 400
      body shouldBe s"The bag with id '$uuid' could not be found"
    }
  }

  it should "fail when the parameter 'bagId' is not present" in {
    put("/bags") {
      status shouldBe 404
    }
  }

  it should "fail when the parameter 'bagId' is not a well-formatted UUID" in {
    put("/bags/abc-def-ghi-jkl-mno") {
      status shouldBe 400
      body shouldBe "invalid UUID string: abc-def-ghi-jkl-mno"
    }
  }

  it should "fail when the parameter 'bagId' is not a UUID" in {
    put("/bags/foobar") {
      status shouldBe 400
      body shouldBe "invalid UUID string: foobar"
    }
  }
}
