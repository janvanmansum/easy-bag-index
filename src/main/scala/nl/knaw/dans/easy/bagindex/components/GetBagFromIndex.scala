package nl.knaw.dans.easy.bagindex.components

import nl.knaw.dans.easy.bagindex.{ BagId, BagRelation }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

trait GetBagFromIndex {
  this: Database with DebugEnhancedLogging =>

  /**
   * Returns a sequence of all bagIds that are in the same bag sequence as the given bagId.
   * The resulting sequence is sorted by the 'created' timestamp.
   * If the given bagId is not in the database, a `BagIdNotFoundException` will be returned.
   *
   * @param bagId the bagId of which the whole sequence is requested
   * @return a sequence of all bagIds that are in the same bag sequence as the given bagId.
   */
  def getBagSequence(bagId: BagId): Try[Seq[BagId]] = {
    trace(bagId)
    for {
      baseId <- getBaseBagId(bagId)
      seq <- getAllBagsWithBase(baseId)
    } yield seq
  }

  /**
   * Returns the `Relation` object for the given bagId if it is present in the database.
   * If the bagId does not exist, a `BagIdNotFoundException` is returned.
   *
   * @param bagId the bagId corresponding to the relation
   * @return the relation data of the given bagId
   */
  def getBagInfo(bagId: BagId): Try[BagRelation] = {
    trace(bagId)
    getBagRelation(bagId)
  }
}
