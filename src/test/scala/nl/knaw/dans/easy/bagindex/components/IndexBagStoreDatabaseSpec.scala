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

import nl.knaw.dans.easy.bagindex.{ BagId, BagIndexDatabaseFixture, BagInfo, TestSupportFixture }
import nl.knaw.dans.lib.error._
import org.joda.time.DateTime

import scala.util.Success

class IndexBagStoreDatabaseSpec extends TestSupportFixture
  with BagIndexDatabaseFixture
  with IndexBagStoreDatabaseComponent
  with DatabaseComponent {

  override val database: Database = new Database {}
  override val indexDatabase: IndexBagStoreDatabase = new IndexBagStoreDatabase {}

  def setupBagStoreIndexTestCase(): Map[Char, (BagId, DateTime)] = {
    // sequence with first bag F
    val bagIdA = UUID.randomUUID()
    val bagIdB = UUID.randomUUID()
    val bagIdC = UUID.randomUUID()
    val bagIdD = UUID.randomUUID()
    val bagIdE = UUID.randomUUID()
    val bagIdF = UUID.randomUUID()
    val bagIdG = UUID.randomUUID()

    // sequence with first bag Z
    val bagIdX = UUID.randomUUID()
    val bagIdY = UUID.randomUUID()
    val bagIdZ = UUID.randomUUID()

    // dates
    val dateA = DateTime.parse("1992")
    val dateB = DateTime.parse("1995")
    val dateC = DateTime.parse("1998")
    val dateD = DateTime.parse("2001")
    val dateE = DateTime.parse("1989")
    val dateF = DateTime.parse("1986")
    val dateG = DateTime.parse("1983")
    val dateX = DateTime.parse("2016")
    val dateY = DateTime.parse("2015")
    val dateZ = DateTime.parse("2014")

    // dois
    val doiA = "10.5072/dans-a6f-kf66"
    val doiB = "10.5072/dans-b6f-kf66"
    val doiC = "10.5072/dans-c6f-kf66"
    val doiD = "10.5072/dans-d6f-kf66"
    val doiE = "10.5072/dans-e6f-kf66"
    val doiF = "10.5072/dans-f6f-kf66"
    val doiG = "10.5072/dans-g6f-kf66"
    val doiX = "10.5072/dans-x6f-kf66"
    val doiY = "10.5072/dans-y6f-kf66"
    val doiZ = "10.5072/dans-z6f-kf66"

    val relations = BagInfo(bagIdA, bagIdE, dateA, doiA) ::
      BagInfo(bagIdB, bagIdA, dateB, doiB) ::
      BagInfo(bagIdC, bagIdA, dateC, doiC) ::
      BagInfo(bagIdD, bagIdB, dateD, doiD) ::
      BagInfo(bagIdE, bagIdF, dateE, doiE) ::
      BagInfo(bagIdF, bagIdF, dateF, doiF) ::
      BagInfo(bagIdG, bagIdC, dateG, doiG) ::
      BagInfo(bagIdX, bagIdY, dateX, doiX) ::
      BagInfo(bagIdY, bagIdZ, dateY, doiY) ::
      BagInfo(bagIdZ, bagIdZ, dateZ, doiZ) :: Nil

    relations.map(info => database.addBagInfo(info.bagId, info.baseId, info.created, info.doi)).collectResults shouldBe a[Success[_]]

    Map(
      'a' -> (bagIdA, dateA),
      'b' -> (bagIdB, dateB),
      'c' -> (bagIdC, dateC),
      'd' -> (bagIdD, dateD),
      'e' -> (bagIdE, dateE),
      'f' -> (bagIdF, dateF),
      'g' -> (bagIdG, dateG),
      'x' -> (bagIdX, dateX),
      'y' -> (bagIdY, dateY),
      'z' -> (bagIdZ, dateZ)
    )
  }

  private def getBagId(char: Char)(implicit bags: Map[Char, (BagId, DateTime)]) = {
    val (bagId, _) = bags(char)
    bagId
  }

  "getAllBaseBagIds" should "return a sequence of bagIds refering to bags that are the base of their sequence" in {
    implicit val bags = setupBagStoreIndexTestCase()

    inside(indexDatabase.getAllBaseBagIds) {
      case Success(bases) => bases should contain allOf(getBagId('f'), getBagId('z'))
    }
  }

  "getAllBagsInSequence" should "return a sequence of bagIds and date/times of all bags that are in the same sequence as the given bagId" in {
    implicit val bags = setupBagStoreIndexTestCase()
    val (zBags, fBags) = bags.partition { case (c, _) => List('x', 'y', 'z').contains(c) }
    val fBag1 :: fBag2 :: fTail = fBags.values.toList
    val zBag1 :: zBag2 :: zTail = zBags.values.toList

    inside(indexDatabase.getAllBagsInSequence(getBagId('f'))) {
      case Success(sequence) => sequence should (have size 7 and contain allOf(fBag1, fBag2, fTail: _*))
    }
    inside(indexDatabase.getAllBagsInSequence(getBagId('z'))) {
      case Success(sequence) => sequence should (have size 3 and contain allOf(zBag1, zBag2, zTail: _*))
    }
  }

  "clearIndex" should "delete all data from the bag-index" in {
    inside(database.getAllBagInfos) {
      case Success(data) => data should not be empty
    }

    indexDatabase.clearIndex() shouldBe a[Success[_]]

    inside(database.getAllBagInfos) {
      case Success(data) => data shouldBe empty
    }
  }

  it should "succeed if clearing an empty bag-index" in {
    inside(database.getAllBagInfos) {
      case Success(data) => data should not be empty
    }

    indexDatabase.clearIndex() shouldBe a[Success[_]]
    indexDatabase.clearIndex() shouldBe a[Success[_]]

    inside(database.getAllBagInfos) {
      case Success(data) => data shouldBe empty
    }
  }

  "updateBagsInSequence" should "update all bags in the sequence to have the newBaseId as their base in the database" in {
    implicit val bags = setupBagStoreIndexTestCase()

    inside(indexDatabase.getAllBagsInSequence(getBagId('f'))) { case Success(xs) =>
      val fBags = xs.map { case (bagId, _) => bagId }
      indexDatabase.updateBagsInSequence(getBagId('g'), fBags) shouldBe a[Success[_]]
    }

    inside(database.getAllBagInfos) {
      case Success(rels) => rels.map(rel => (rel.bagId, rel.baseId)) should contain allOf(
        (getBagId('a'), getBagId('g')),
        (getBagId('b'), getBagId('g')),
        (getBagId('c'), getBagId('g')),
        (getBagId('d'), getBagId('g')),
        (getBagId('e'), getBagId('g')),
        (getBagId('f'), getBagId('g')),
        (getBagId('g'), getBagId('g')),
        // x, y and z should be untouched
        (getBagId('x'), getBagId('y')),
        (getBagId('y'), getBagId('z')),
        (getBagId('z'), getBagId('z'))
      )
    }
  }
}
