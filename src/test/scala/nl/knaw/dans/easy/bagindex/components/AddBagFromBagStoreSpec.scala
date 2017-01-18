package nl.knaw.dans.easy.bagindex.components

import java.util.UUID

import nl.knaw.dans.easy.bagindex._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.{ Failure, Success }

class AddBagFromBagStoreSpec extends BagStoreFixture with BagIndexDatabaseFixture with Bagit4FacadeComponent with AddBagToIndex with AddBagFromBagStore {

  private def assertBagRelationNotInDatabase(bagId: BagId): Unit = {
    inside(getBagRelation(bagId)) {
      case Failure(BagIdNotFoundException(id)) => id shouldBe bagId
    }
  }

  private def assertBagRelationInDatabase(relation: BagRelation): Unit = {
    inside(getBagRelation(relation.bagId)) {
      case Success(BagRelation(id, base, created)) =>
        id shouldBe relation.bagId
        base shouldBe relation.baseId
        created.toString(dateTimeFormatter) shouldBe relation.created.toString(dateTimeFormatter)
    }
  }

  private def assertAdditionReturnedExpectedBaseId(bagId: BagId, baseId: BaseId): Unit = {
    inside(addFromBagStore(bagId)) {
      case Success(base) => base shouldBe baseId
    }
  }

  def addBaseTest(): Unit = {
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000001")

    assertBagRelationNotInDatabase(bagId)

    assertAdditionReturnedExpectedBaseId(bagId, bagId)

    assertBagRelationInDatabase(BagRelation(bagId, bagId, DateTime.parse("2017-01-16T14:35:00.888+01:00", ISODateTimeFormat.dateTime())))
  }

  def addDirectChildTest(): Unit = {
    addBaseTest()
    val baseId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000002")

    assertBagRelationNotInDatabase(bagId)

    assertAdditionReturnedExpectedBaseId(bagId, baseId)

    assertBagRelationInDatabase(BagRelation(bagId, baseId, DateTime.parse("2017-01-17T14:35:00.888+01:00", ISODateTimeFormat.dateTime())))
  }

  def addIndirectChildtest(): Unit = {
    addDirectChildTest()
    val superBaseId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000003")

    assertBagRelationNotInDatabase(bagId)

    assertAdditionReturnedExpectedBaseId(bagId, superBaseId)

    assertBagRelationInDatabase(BagRelation(bagId, superBaseId, DateTime.parse("2017-01-18T14:35:00.888+01:00", ISODateTimeFormat.dateTime())))
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

    inside(addFromBagStore(bagId)) {
      case Failure(BagNotFoundInBagStoreException(id, bagStore)) =>
        id shouldBe bagId
        bagStore shouldBe bagStoreBaseDir
    }
  }
}
