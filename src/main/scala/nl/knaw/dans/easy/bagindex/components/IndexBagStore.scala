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

import java.sql.{ SQLException, Savepoint }
import java.util.UUID

import nl.knaw.dans.easy.bagindex.{ BagId, BagInfo, BaseId, _ }
import nl.knaw.dans.lib.error.TraversableTryExtensions
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime
import resource.managed

import scala.collection.immutable.Seq
import scala.util.{ Failure, Try }

trait IndexBagStore {
  this: BagStoreAccess with BagFacadeComponent with IndexBagStoreDatabase with Database =>

  implicit private def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)

  /**
   * Traverse the bagstore and create an index of bag relations based on the bags inside.
   *
   * @return `Success` if the indexing was successful; `Failure` otherwise
   */
  def indexBagStore(): Try[Unit] = {
    for {
      savepoint <- startTransaction
      action = for {
        // delete all data from the bag-index
        _ <- clearIndex()
        // walk over bagstore
        bags <- traverse
        // extract data from bag-info.txt
        infos = bags.map {
          case (bagId, path) =>
            bagFacade.getIndexRelevantBagInfo(path).get match {
              // TODO is there a better way to fail fast?
              case (Some(baseDir), Some(created)) => BagInfo(bagId, baseDir, created)
              case (None, Some(created)) => BagInfo(bagId, bagId, created)
              case _ => throw new Exception(s"could not index bag $bagId")
            }
        }
        // insert data 'as-is'
        _ <- Try {
          // TODO is there a better way to fail fast?
          // - Richard: "Yes, there is, because you're working on a Stream. I'll add it to the dans-scala-lib as soon as I have time for it."
          infos.foreach(relation => addBagInfo(relation.bagId, relation.baseId, relation.created).get)
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
      _ <- completeTransaction(action, savepoint)
    } yield ()
  }
}

/**
 * A separate set of database queries, specially written for `IndexBagStore`.
 * Some of these functions can invalidate the bag-index and are therefore not added to the
 * usual `Database` component.
 */
trait IndexBagStoreDatabase {
  this: DatabaseAccess with DebugEnhancedLogging =>

  /**
   * Start a transaction and return a `Savepoint` for potential rollback.
   *
   * @return a `Savepoint`
   */
  def startTransaction: Try[Savepoint] = Try {
    connection.setAutoCommit(false)
    connection.setSavepoint()
  }

  /**
   * Complete a transaction by either committing the queries if `result` is a `Success`,
   * or by rolling back the changes if `result` is a `Failure`.
   *
   * @param result the result of the database actions taken thusfar
   * @param savepoint the database status to which to roll back in case `result` is a `Failure`
   * @tparam T the type of result
   * @return the same result if completing the transaction was successful; `Failure` otherwise
   */
  def completeTransaction[T](result: Try[T], savepoint: Savepoint): Try[T] = {
    result
      .ifFailure {
        case e: SQLException => Try { connection.rollback(savepoint) }.flatMap(_ => Failure(e))
      }
      .ifSuccess(_ => {
        connection.commit()
        connection.setAutoCommit(true)
      })
  }

  /**
   * Return a sequence of bagIds refering to bags that are the base of their sequence.
   *
   * @return the bagId of the base of every sequence
   */
  def getAllBaseBagIds: Try[Seq[BagId]] = {
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
   * @return the sequence of bagIds and creation times that are in the same bag-sequence as the given bagId
   */
  def getAllBagsInSequence(bagId: BagId): Try[Seq[(BagId, DateTime)]] = {
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
   * @return `Success` if all data was deleted; `Failure` otherwise
   */
  def clearIndex(): Try[Unit] = Try {
    managed(connection.createStatement)
      .acquireAndGet(_.executeUpdate("DELETE FROM bag_info;"))
  }

  /**
   * Update all records for which the bagId is in the given `bagSequence` to have `newBaseId`
   * as their base.
   *
   * @param newBaseId the new baseId to be put in the bag-index
   * @param bagSequence the sequence of bagIds to be updated
   * @return `Success` if the update was successful; `Failure` otherwise
   */
  def updateBagsInSequence(newBaseId: BaseId, bagSequence: Seq[BagId]): Try[Unit] = {
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
