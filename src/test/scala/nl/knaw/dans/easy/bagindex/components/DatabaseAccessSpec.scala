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

import java.sql.SQLException
import java.util.UUID

import nl.knaw.dans.easy.bagindex.BagIndexDatabaseFixture
import org.joda.time.DateTime

import scala.util.{ Failure, Success, Try }

class DatabaseAccessSpec extends BagIndexDatabaseFixture with Database {

  "doTransaction" should "succeed when the arg returns a Success" in {
    inside(doTransaction(_ => Success("foo"))) {
      case Success(s) => s shouldBe "foo"
    }
  }

  it should "fail when the arg function returns a Failure" in {
    inside(doTransaction(_ => Failure(new Exception("error message")))) {
      case Failure(e) => e.getMessage shouldBe "error message"
    }
  }

  it should "fail when the arg function closes the connection" in {
    inside(doTransaction(c => Try { c.close() })) {
      case Failure(e) => e shouldBe a[SQLException]
    }
  }

  it should "rollback changes made to the database whenever an error occurs in the arg function" in {
    val bagId = UUID.randomUUID()

    val originalContent = getAllBagInfos
    inside(originalContent) {
      case Success(infos) => infos.map(_.bagId) should not contain bagId
    }

    inside(doTransaction(implicit c => {
      val add = addBagInfo(bagId, bagId, DateTime.now)(c)
      add shouldBe a[Success[_]]

      // check that the bag was added properly
      inside(getAllBagInfos(c)) {
        case Success(infos) =>
          infos.map(_.bagId) should contain(bagId)
      }

      // based on this failure a rollback should happen
      Failure(new Exception("random exception"))
    })) {
      case Failure(e) => e.getMessage shouldBe "random exception"
    }

    // the current content should equal the old content
    val newContent = getAllBagInfos
    newContent shouldBe originalContent
    inside(newContent) {
      case Success(infos) => infos.map(_.bagId) should not contain bagId
    }
  }

  it should "rollback changes made to the database whenever an error occurs in the post arg func phase" in {
    val bagId = UUID.randomUUID()

    val originalContent = getAllBagInfos
    inside(originalContent) {
      case Success(infos) => infos.map(_.bagId) should not contain bagId
    }

    inside(doTransaction(implicit c => {
      val add = addBagInfo(bagId, bagId, DateTime.now)(c)
      add shouldBe a[Success[_]]

      // check that the bag was added properly
      inside(getAllBagInfos(c)) {
        case Success(infos) =>
          infos.map(_.bagId) should contain(bagId)
      }

      // based on this a failure occurs on commit
      c.close()

      Success(())
    })) {
      case Failure(e) => e shouldBe a[SQLException]
    }

    // the current content should equal the old content
    val newContent = getAllBagInfos
    newContent shouldBe originalContent
    inside(newContent) {
      case Success(infos) => infos.map(_.bagId) should not contain bagId
    }
  }
}
