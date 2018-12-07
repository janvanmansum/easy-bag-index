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

import java.sql.{ Connection, PreparedStatement, ResultSet, SQLException }
import java.util.UUID

import nl.knaw.dans.easy.bagindex._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime
import resource._

import scala.collection.immutable.Seq
import scala.util.{ Failure, Try }

trait DatabaseComponent extends DebugEnhancedLogging {

  val database: Database

  private class Query(query: String, prepare: PreparedStatement => Unit)(implicit connection: Connection) {

    val resultSet: ManagedResource[ResultSet] = for {
      prepStatement <- managed(connection.prepareStatement(query))
      _ = prepare(prepStatement)
      resultSet <- managed(prepStatement.executeQuery())
    } yield resultSet

    def select[T](resultToObject: ResultSet => T)(onFailure: () => T): Try[T] = {
      resultSet.map(result => if (result.next()) resultToObject(result)
                              else onFailure()).tried
    }

    def selectMany[T](resultToObject: ResultSet => T): Try[Seq[T]] = {
      resultSet
        .map(result => Stream.continually(result.next())
          .takeWhile(b => b)
          .map(_ => resultToObject(result))
          .toList)
        .tried
    }
  }
  private object Query {
    def apply(query: String)(prepare: PreparedStatement => Unit)(implicit connection: Connection): Query = {
      new Query(query, prepare)
    }
  }

  trait Database {
    private def getBagInfo(result: ResultSet): BagInfo = {
      println(s"getBagInfo '${ result.getString("created") }'")

      BagInfo(
        bagId = UUID.fromString(result.getString("bagId").trim),
        baseId = UUID.fromString(result.getString("base").trim),
        created = DateTime.parse(result.getString("created").trim, dateTimeFormatter),
        doi = result.getString("doi").trim)
    }

    private def getBagId(result: ResultSet): BagId = {
      UUID.fromString(result.getString("bagId"))
    }

    private def getBaseId(result: ResultSet): BaseId = {
      UUID.fromString(result.getString("base"))
    }

    /**
     * Return the baseId of the given bagId if the latter exists.
     * If the bagId does not exist, a `BagIdNotFoundException` is returned.
     *
     * @param bagId      the bagId for which the base bagId needs to be returned
     * @param connection the connection to the database on which this query needs to be run
     * @return the baseId of the given bagId if it exists; failure otherwise
     */
    def getBaseBagId(bagId: BagId)(implicit connection: Connection): Try[BaseId] = {
      trace(bagId)
      Query("SELECT base FROM bag_info WHERE bagId=?;")(_.setString(1, bagId.toString))
        .select(getBaseId)(() => throw BagIdNotFoundException(bagId))
    }

    /**
     * Returns a sequence of all bagIds that have the given baseId as their base, ordered by the 'created' timestamp.
     *
     * @param baseId     the baseId used during this search
     * @param connection the connection to the database on which this query needs to be run
     * @return a sequence of all bagIds with a given baseId
     */
    def getAllBagsWithBase(baseId: BaseId)(implicit connection: Connection): Try[Seq[BagId]] = {
      trace(baseId)
      Query("SELECT bagId FROM bag_info WHERE base=? ORDER BY created;")(_.setString(1, baseId.toString))
        .selectMany(getBagId)
    }

    /**
     * Returns the `Relation` object for the given bagId if it is present in the database.
     * If the bagId does not exist, a `BagIdNotFoundException` is returned.
     *
     * @param bagId      the bagId corresponding to the relation
     * @param connection the connection to the database on which this query needs to be run
     * @return the relation data of the given bagId
     */
    def getBagInfo(bagId: BagId)(implicit connection: Connection): Try[BagInfo] = {
      trace(bagId)
      Query("SELECT bagId, base, created, doi FROM bag_info WHERE bagId=?;")(_.setString(1, bagId.toString))
        .select(getBagInfo)(() => throw BagIdNotFoundException(bagId))
    }

    /**
     * Returns a sequence of all bag relations that have a given `DOI`.
     *
     * @param doi        the DOI to be searched
     * @param connection the connection to the database on which the query needs to be run
     * @return a list of bag relations with a given `DOI`
     */
    def getBagsWithDoi(doi: Doi)(implicit connection: Connection): Try[Seq[BagInfo]] = {
      trace(doi)
      Query("SELECT bagId, base, created, doi FROM bag_info WHERE doi=?;")(_.setString(1, doi)).selectMany(getBagInfo)
    }

    /**
     * Returns a sequence of all bag relations that are present in the database.
     * '''Warning:''' this may load large amounts of data into memory.
     *
     * @param connection the connection to the database on which this query needs to be run
     * @return a list of all bag relations
     */
    def getAllBagInfos(implicit connection: Connection): Try[Seq[BagInfo]] = {
      Query("SELECT bagId, base, created, doi FROM bag_info;")(_ => ()).selectMany(getBagInfo)
    }

    /**
     * Add a bag relation to the database. A bag relation consists of a unique bagId (that is not yet
     * included in the database), a base bagId and a 'created' timestamp.
     *
     * @param bagId      the unique bag identifier
     * @param baseId     the base bagId of the bagId
     * @param created    the date/time at which the bag was created
     * @param connection the connection to the database on which this action needs to be applied
     * @return `Success` if the bag relation was added successfully; `Failure` otherwise
     */
    def addBagInfo(bagId: BagId, baseId: BaseId, created: DateTime, doi: Doi)(implicit connection: Connection): Try[Unit] = {
      trace(bagId, baseId, created)

      managed(connection.prepareStatement("INSERT INTO bag_info VALUES (?, ?, ?, ?);"))
        .map(prepStatement => {
          prepStatement.setString(1, bagId.toString)
          prepStatement.setString(2, baseId.toString)
          val createdString = created.toString(dateTimeFormatter)
          println(s"addBagInfo '$createdString'")
          prepStatement.setString(3, createdString)
          prepStatement.setString(4, doi)
          prepStatement.executeUpdate()
        })
        .tried
        .map(_ => ())
        .recoverWith {
          case e: SQLException if e.getMessage.toLowerCase contains "unique constraint" =>
            Failure(BagAlreadyInIndexException(bagId))
        }
    }
  }
}
