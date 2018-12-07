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

import nl.knaw.dans.easy.bagindex.{ BagId, BaseId, dateTimeFormatter }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime
import resource.managed

import scala.collection.immutable.Seq
import scala.util.Try

trait IndexBagStoreDatabaseComponent extends DatabaseComponent with DebugEnhancedLogging {

  val indexDatabase: IndexBagStoreDatabase

  /**
   * A separate set of database queries, specially written for `IndexBagStore`.
   * Some of these functions can invalidate the bag-index and are therefore not added to the
   * usual `Database` component.
   */
  trait IndexBagStoreDatabase {

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
     * @param bagId      the bagId for which the rest of the sequence needs to be found
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

      println(s"'$query'")

      val resultSet = for {
        prepStatement <- managed(connection.prepareStatement(query))
        _ = prepStatement.setString(1, bagId.toString)
        resultSet <- managed(prepStatement.executeQuery())
      } yield resultSet

      resultSet
        .map(result => Stream.continually(result.next())
          .takeWhile(b => b)
          .map(_ => {
            /* The `.trim` in the expression below is to remove any and all trailing whitespaces
             * from this query result. These whitespaces only occur when the tests are run on Travis
             * and only with the query above. Other queries that 'SELECT created' don't show this
             * behavior.
             */
            val created = result.getString("created").trim
            println(s"getAllBagsInSequence: '$created'")

            (UUID.fromString(result.getString("bagId")), DateTime.parse(created, dateTimeFormatter))
          })
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
     * @param newBaseId   the new baseId to be put in the bag-index
     * @param bagSequence the sequence of bagIds to be updated
     * @param connection  the connection to the database on which this query needs to be run
     * @return `Success` if the update was successful; `Failure` otherwise
     */
    def updateBagsInSequence(newBaseId: BaseId, bagSequence: Seq[BagId])(implicit connection: Connection): Try[Unit] = {
      trace(newBaseId, bagSequence)

      val query = s"UPDATE bag_info SET base = ? WHERE bagId IN (${ bagSequence.map(_ => "?").mkString(", ") });"
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
}
