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

import nl.knaw.dans.easy.bagindex.BagId
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

trait GetBagFromIndex {
  this: Database
    with DebugEnhancedLogging =>

  /**
   * Returns a sequence of all bagIds that are in the same bag sequence as the given bagId.
   * The resulting sequence is sorted by the 'created' timestamp.
   * If the given bagId is not in the database, a `BagIdNotFoundException` will be returned.
   *
   * @param bagId the bagId of which the whole sequence is requested
   * @param connection the connection to the database on which this query needs to be run
   * @return a sequence of all bagIds that are in the same bag sequence as the given bagId.
   */
  def getBagSequence(bagId: BagId)(implicit connection: Connection): Try[Seq[BagId]] = {
    trace(bagId)
    for {
      baseId <- getBaseBagId(bagId)
      seq <- getAllBagsWithBase(baseId)
    } yield seq
  }
}
