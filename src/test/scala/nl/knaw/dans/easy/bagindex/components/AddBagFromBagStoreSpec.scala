/**
 * Copyright (C) 2017 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.bagindex.components

import java.util.UUID

import nl.knaw.dans.easy.bagindex._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.{ Failure, Success }

class AddBagFromBagStoreSpec extends BagStoreFixture with BagIndexDatabaseFixture with Bagit4Fixture with AddBagToIndex with AddBagFromBagStore {

  private def assertBagInfoNotInDatabase(bagId: BagId): Unit = {
    getBagInfo(bagId) should matchPattern { case Failure(BagIdNotFoundException(`bagId`)) => }
  }

  private def assertBagInfoInDatabase(relation: BagInfo): Unit = {
    inside(getBagInfo(relation.bagId)) {
      case Success(BagInfo(id, base, created, doi)) =>
        id shouldBe relation.bagId
        base shouldBe relation.baseId
        created.toString(dateTimeFormatter) shouldBe relation.created.toString(dateTimeFormatter)
        doi shouldBe relation.doi
    }
  }

  private def assertAdditionReturnedExpectedBaseId(bagId: BagId, baseId: BaseId): Unit = {
    addFromBagStore(bagId) should matchPattern { case Success(`baseId`) => }
  }

  def addBaseTest(): Unit = {
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val doi = "10.5072/dans-2xg-umq8"

    assertBagInfoNotInDatabase(bagId)

    assertAdditionReturnedExpectedBaseId(bagId, bagId)

    assertBagInfoInDatabase(BagInfo(bagId, bagId, DateTime.parse("2017-01-16T14:35:00.888+01:00", ISODateTimeFormat.dateTime()), doi))
  }

  def addDirectChildTest(): Unit = {
    addBaseTest()
    val baseId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val doi = "10.5072/dans-2xg-umq9"

    assertBagInfoNotInDatabase(bagId)

    assertAdditionReturnedExpectedBaseId(bagId, baseId)

    assertBagInfoInDatabase(BagInfo(bagId, baseId, DateTime.parse("2017-01-17T14:35:00.888+01:00", ISODateTimeFormat.dateTime()), doi))
  }

  def addIndirectChildtest(): Unit = {
    addDirectChildTest()
    val superBaseId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000003")
    val doi = "10.5072/dans-2xg-umq0"

    assertBagInfoNotInDatabase(bagId)

    assertAdditionReturnedExpectedBaseId(bagId, superBaseId)

    assertBagInfoInDatabase(BagInfo(bagId, superBaseId, DateTime.parse("2017-01-18T14:35:00.888+01:00", ISODateTimeFormat.dateTime()), doi))
  }

  // add base bag
  "addFromBagStore" should "add a bagId to the database as a base bagId when no IS_VERSION_OF is specified in the bag-info.txt" in {
    addBaseTest()
  }

  // add revision with direct base
  it should "add a bagId to the database with a base bagId when the correct super-baseId is specified in the bag-info.txt" in {
    addDirectChildTest()
  }

  // add revision with indirect base
  it should "add a bagId to the database with another bagId as its base when the incorrect super-baseId is specified in the bag-info.txt" in {
    addIndirectChildtest()
  }

  // add with invalid bagId
  it should "fail when the bagId is not found in the bagstore" in {
    val bagId = UUID.randomUUID()
    addFromBagStore(bagId) should matchPattern { case Failure(BagNotFoundException(`bagId`)) => }
  }
}
