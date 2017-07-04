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

import nl.knaw.dans.easy.bagindex.BagInfo
import nl.knaw.dans.easy.bagindex.access.{ BagFacadeComponent, BagStoreAccessComponent }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime

import scala.util.Try

trait IndexBagStoreComponent {
  this: BagStoreAccessComponent
    with BagFacadeComponent
    with IndexBagStoreDatabaseComponent
    with DatabaseComponent
    with DebugEnhancedLogging =>

  val indexFull: IndexBagStore

  trait IndexBagStore {

    implicit private def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

    /**
     * Traverse the bagstore and create an index of bag relations based on the bags inside.
     *
     * @param connection the connection to the database on which this query needs to be run
     * @return `Success` if the indexing was successful; `Failure` otherwise
     */
    def indexBagStore()(implicit connection: Connection): Try[Unit] = {
      trace(())
      for {
      // delete all data from the bag-index
        _ <- indexDatabase.clearIndex()
        // walk over bagstore
        bags <- bagStore.traverse
        // extract data from bag-info.txt
        infos = bags.map {
          case (bagId, path) =>
            (bagFacade.getIndexRelevantBagInfo(path).get, bagFacade.getDoi(bagStore.toDatasetXml(path, bagId)).get) match {
              // TODO is there a better way to fail fast?
              case ((Some(baseDir), Some(created)), doi) => BagInfo(bagId, baseDir, created, doi)
              case ((None, Some(created)), doi) => BagInfo(bagId, bagId, created, doi)
              case _ => throw new Exception(s"could not index bag $bagId")
            }
        }
        // insert data 'as-is'
        _ <- Try {
          // TODO is there a better way to fail fast?
          // - ~~Richard: "Yes, there is, because you're working on a Stream. I'll add it to the dans-scala-lib as soon as I have time for it."~~
          // - Richard: "Streams were not really designed to interact with Try, like we do with Seq/List/etc. This would however work with an Observable!"
          for (relation <- infos) {
            logger.info(s"adding relation: $relation")
            database.addBagInfo(relation.bagId, relation.baseId, relation.created, relation.doi).get
          }
        }
        // get all base bagIds
        bases <- indexDatabase.getAllBaseBagIds
        // get the bags in the same collection as the base bagId and calculate the oldest one
        oldestBagInSequence <- bases.map(baseId => {
          for {
            collection <- indexDatabase.getAllBagsInSequence(baseId)
            (oldestBagId, _) = collection.minBy { case (_, created) => created }
            bagIds = collection.map { case (bagId, _) => bagId }
          } yield (oldestBagId, bagIds)
        }).collectResults
        // perform update query for each collection
        _ <- Try {
          // TODO is there a better way to fail fast?
          oldestBagInSequence.foreach { case (oldest, sequence) => indexDatabase.updateBagsInSequence(oldest, sequence).get }
        }
      } yield ()
    }
  }
}
