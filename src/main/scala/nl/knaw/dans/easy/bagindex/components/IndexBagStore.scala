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
import java.util.UUID

import nl.knaw.dans.easy.bagindex.{ BagId, BagInfo, BaseId, _ }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime
import resource.managed

import scala.collection.immutable.Seq
import scala.util.Try

trait IndexBagStore {
  this: BagStoreAccess
    with BagFacadeComponent
    with IndexBagStoreDatabase
    with Database
    with DebugEnhancedLogging =>

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
      _ <- clearIndex()
      // walk over bagstore
      bags <- traverse
      // extract data from bag-info.txt
      infos = bags.map {
        case (bagId, path) =>
          (bagFacade.getIndexRelevantBagInfo(path).get, bagFacade.getDoi(toDatasetXml(path, bagId)).get) match {
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
          addBagInfo(relation.bagId, relation.baseId, relation.created, relation.doi).get
        }
      }
      // get all base bagIds
      bases <- getAllBaseBagIds
      // get the bags in the same collection as the base bagId and calculate the oldest one
      oldestBagInSequence <- bases.map(baseId => {
        for {
          collection <- getAllBagsInSequence(baseId)
          (oldestBagId, _) = collection.minBy { case (_, created) => created }
          bagIds = collection.map { case (bagId, _) => bagId }
        } yield (oldestBagId, bagIds)
      }).collectResults
      // perform update query for each collection
      _ <- Try {
        // TODO is there a better way to fail fast?
        oldestBagInSequence.foreach { case (oldest, sequence) => updateBagsInSequence(oldest, sequence).get }
      }
    } yield ()
  }
}

/**
 * A separate set of database queries, specially written for `IndexBagStore`.
 * Some of these functions can invalidate the bag-index and are therefore not added to the
 * usual `Database` component.
 */
trait IndexBagStoreDatabase {
  this: DebugEnhancedLogging =>

  /**
   * Return a sequence of bagIds refering to bags that are the base of their sequence.
   *
   * @param connection the connection to the database on which this query needs to be run
   * @return the bagId of the base of every sequence
   */
  def getAllBaseBagIds(implicit connection: Connection): Try[Seq[BagId]] = {
    trace(())
    val resultSet = for {
      statement <- managed(connection.createStatement)
      resultSet <- managed(statement.executeQuery("SELECT bagId FROM bag_info WHERE bagId = base;"))
    } yield resultSet

    resultSet
      .map(result => Stream.continually(result.next())
        .takeWhile(b => b)
        .map(_ => UUID.fromString(result.getString("bagId")))
        .toList)
      .tried
  }

  /**
   * Return a sequence of bagIds (refering to bags in the bag-store) and their creation time
   * from the bag-index that are in the same bag-sequence as the given bagId.
   *
   * @param bagId the bagId for which the rest of the sequence needs to be found
   * @param connection the connection to the database on which this query needs to be run
   * @return the sequence of bagIds and creation times that are in the same bag-sequence as the given bagId
   */
  def getAllBagsInSequence(bagId: BagId)(implicit connection: Connection): Try[Seq[(BagId, DateTime)]] = {
    trace(bagId)

    val query =
      """
        |WITH RECURSIVE bags_in_sequence(bag) AS (
        |    VALUES(?)
        |    UNION SELECT bagId
        |          FROM bag_info JOIN bags_in_sequence ON bag_info.base = bags_in_sequence.bag
        |)
        |SELECT bagId, created
        |FROM bag_info JOIN bags_in_sequence ON bag_info.bagId = bags_in_sequence.bag;
      """.stripMargin

    val resultSet = for {
      prepStatement <- managed(connection.prepareStatement(query))
      _ = prepStatement.setString(1, bagId.toString)
      resultSet <- managed(prepStatement.executeQuery())
    } yield resultSet

    resultSet
      .map(result => Stream.continually(result.next())
        .takeWhile(b => b)
        .map(_ => (UUID.fromString(result.getString("bagId")), DateTime.parse(result.getString("created"), dateTimeFormatter)))
        .toList)
      .tried
  }

  /**
   * Delete all data from the bag-index.
   *
   * @param connection the connection to the database on which this query needs to be run
   * @return `Success` if all data was deleted; `Failure` otherwise
   */
  def clearIndex()(implicit connection: Connection): Try[Unit] = Try {
    trace(())
    managed(connection.createStatement)
      .acquireAndGet(_.executeUpdate("DELETE FROM bag_info;"))

    logger.info("index cleared")
  }

  /**
   * Update all records for which the bagId is in the given `bagSequence` to have `newBaseId`
   * as their base.
   *
   * @param newBaseId the new baseId to be put in the bag-index
   * @param bagSequence the sequence of bagIds to be updated
   * @param connection the connection to the database on which this query needs to be run
   * @return `Success` if the update was successful; `Failure` otherwise
   */
  def updateBagsInSequence(newBaseId: BaseId, bagSequence: Seq[BagId])(implicit connection: Connection): Try[Unit] = {
    trace(newBaseId, bagSequence)

    val query = s"UPDATE bag_info SET base = ? WHERE bagId IN (${bagSequence.map(_ => "?").mkString(", ")});"
    managed(connection.prepareStatement(query))
      .map(prepStatement => {
        prepStatement.setString(1, newBaseId.toString)
        bagSequence.zipWithIndex.foreach { case (bagId, i) => prepStatement.setString(i + 2, bagId.toString) }
        prepStatement.executeUpdate()
      })
      .tried
      .map(_ => ())
  }
}
