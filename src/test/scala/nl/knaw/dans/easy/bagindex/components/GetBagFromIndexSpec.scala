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
package nl.knaw.dans.easy.bagindex.components

import java.util.UUID

import nl.knaw.dans.easy.bagindex.{ BagIdNotFoundException, BagIndexDatabaseFixture }
import nl.knaw.dans.lib.error._
import org.joda.time.DateTime

import scala.util.{ Failure, Success }

class GetBagFromIndexSpec extends BagIndexDatabaseFixture with GetBagFromIndex {

  "getBagSequence" should "return a sequence with only the baseId when there are no child bags declared" in {
    val bagId = UUID.randomUUID()
    val time = DateTime.now()

    addBagInfo(bagId, bagId, time) shouldBe a[Success[_]]

    inside(getBagSequence(bagId)) {
      case Success(ids) => ids should (have size 1 and contain only bagId)
    }
  }

  it should "return a sequence with all bagIds with a certain baseId when given this baseId" in {
    val bagIds@(baseId :: _) = List.fill(3)(UUID.randomUUID())
    val times = List(
      DateTime.parse("1992-07-30T16:00:00"),
      DateTime.parse("2004-01-01"),
      DateTime.now()
    )

    bagIds.zip(times)
      .map { case (bagId, time) => addBagInfo(bagId, baseId, time) }
      .collectResults shouldBe a[Success[_]]

    inside(getBagSequence(baseId)) {
      case Success(ids) => ids should (have size 3 and contain theSameElementsInOrderAs bagIds)
    }
  }

  it should "return a sequence with all bagIds with a certain baseId when given any of the contained bagIds" in {
    val bagIds@(baseId :: _) = List.fill(3)(UUID.randomUUID())
    val times = List(
      DateTime.parse("1992-07-30T16:00:00"),
      DateTime.parse("2004-01-01"),
      DateTime.now()
    )

    bagIds.zip(times)
      .map { case (bagId, time) => addBagInfo(bagId, baseId, time) }
      .collectResults shouldBe a[Success[_]]

    for (bagId <- bagIds) {
      inside(getBagSequence(bagId)) {
        case Success(ids) => ids should (have size 3 and contain theSameElementsInOrderAs bagIds)
      }
    }
  }

  it should "fail if the given bagId is not present in the database" in {
    // Note: the database is empty at this point!
    val someOtherBagId = UUID.randomUUID()
    inside(getBagSequence(someOtherBagId)) {
      case Failure(BagIdNotFoundException(id)) => id shouldBe someOtherBagId
    }
  }
}
