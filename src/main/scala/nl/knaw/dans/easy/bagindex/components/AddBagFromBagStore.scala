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

import java.sql.Connection

import nl.knaw.dans.easy.bagindex.{ BagId, BaseId }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

trait AddBagFromBagStore {
  this: AddBagToIndex
    with BagStoreAccess
    with BagFacadeComponent
    with DebugEnhancedLogging =>

  /**
   * Add the bag info of the bag identified with bagId to the database.
   * Specificly, the 'Is-Version-Of' and 'Created' fields from the bag's `bag-info.txt` are read
   * and added to the database. If the 'Is-Version-Of' points to a bag that is not a base bag,
   * the ''real'' base bag is related with this bagId instead.
   *
   * @param bagId the bagId identifying the bag to be indexed
   * @param connection the connection to the database on which this action needs to be applied
   * @return the baseId that linked with the given bagId
   */
  def addFromBagStore(bagId: BagId)(implicit connection: Connection): Try[BaseId] = {
    trace(bagId)
    for {
      bagDir <- toLocation(bagId)
      (baseId, created) <- bagFacade.getIndexRelevantBagInfo(bagDir)
      superBaseId <- baseId.map(add(bagId, _, created)).getOrElse(addBase(bagId, created))
    } yield superBaseId
  }
}
