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

    addBagRelation(bagId, bagId, time) shouldBe a[Success[_]]

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
      .map { case (bagId, time) => addBagRelation(bagId, baseId, time) }
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
      .map { case (bagId, time) => addBagRelation(bagId, baseId, time) }
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
