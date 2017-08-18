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

import java.sql.Connection

import nl.knaw.dans.easy.bagindex.access.{ BagFacadeComponent, BagStoreAccessComponent }
import nl.knaw.dans.easy.bagindex.{ BagId, BaseId, Doi }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime

import scala.util.Try

trait IndexBagComponent extends DebugEnhancedLogging {
  this: DatabaseComponent
    with BagStoreAccessComponent
    with BagFacadeComponent =>

  val index: IndexBag

  trait IndexBag {

    /**
     * Returns a sequence of all bagIds that are in the same bag sequence as the given bagId.
     * The resulting sequence is sorted by the 'created' timestamp.
     * If the given bagId is not in the database, a `BagIdNotFoundException` will be returned.
     *
     * @param bagId      the bagId of which the whole sequence is requested
     * @param connection the connection to the database on which this query needs to be run
     * @return a sequence of all bagIds that are in the same bag sequence as the given bagId.
     */
    def getBagSequence(bagId: BagId)(implicit connection: Connection): Try[Seq[BagId]] = {
      trace(bagId)
      for {
        baseId <- database.getBaseBagId(bagId)
        seq <- database.getAllBagsWithBase(baseId)
      } yield seq
    }

    /**
     * Inserts a baseId into the index; that is, the bagId specifies itself as its base.
     *
     * @param bagId      the bagId to be added to the index
     * @param created    the date/time at which the bag corresponding to the bagId was created
     * @param connection the connection to the database on which this action needs to be applied
     * @return `Success` if the bagId was added to the index; `Failure` otherwise
     */
    def addBase(bagId: BagId, created: Option[DateTime] = None, doi: Doi)(implicit connection: Connection): Try[BaseId] = {
      trace(bagId, created)
      database.addBagInfo(bagId, bagId, created.getOrElse(DateTime.now()), doi).map(_ => bagId)
    }

    /**
     * Insert a bagId into the index, given another bagId as its base.
     * If the baseId is already in the index with another baseId, this ''super-baseId'' is used
     * as the baseId of the currently added bagId instead.
     * If the baseId does not exist in the index a `BagIdNotFoundException` is returned.
     *
     * @param bagId      the bagId to be added to the index
     * @param baseId     the base of this bagId
     * @param created    the date/time at which the bag corresponding to the bagId was created
     * @param connection the connection to the database on which this action needs to be applied
     * @return the baseId of the super-base if the bagId was added to the index; `Failure` otherwise
     */
    def add(bagId: BagId, baseId: BaseId, created: Option[DateTime] = None, doi: Doi)(implicit connection: Connection): Try[BaseId] = {
      trace(bagId, baseId, created)
      for {
        superBase <- database.getBaseBagId(baseId)
        _ <- database.addBagInfo(bagId, superBase, created.getOrElse(DateTime.now()), doi)
      } yield superBase
    }

    /**
     * Add the bag info of the bag identified with bagId to the database.
     * Specificly, the 'Is-Version-Of' and 'Created' fields from the bag's `bag-info.txt` are read
     * and added to the database. If the 'Is-Version-Of' points to a bag that is not a base bag,
     * the ''real'' base bag is related with this bagId instead.
     *
     * @param bagId      the bagId identifying the bag to be indexed
     * @param connection the connection to the database on which this action needs to be applied
     * @return the baseId that linked with the given bagId
     */
    def addFromBagStore(bagId: BagId)(implicit connection: Connection): Try[BaseId] = {
      trace(bagId)
      for {
        bagDir <- bagStore.toLocation(bagId)
        (baseId, created) <- bagFacade.getIndexRelevantBagInfo(bagDir)
        datasetXMLPath = bagStore.toDatasetXml(bagDir, bagId)
        doi <- bagFacade.getDoi(datasetXMLPath)
        superBaseId <- baseId.map(add(bagId, _, created, doi)).getOrElse(addBase(bagId, created, doi))
      } yield superBaseId
    }
  }
}
