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

import java.sql.{ Connection, DriverManager }
import java.util.UUID

import nl.knaw.dans.easy.bagindex._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime
import resource._

import scala.collection.immutable.Seq
import scala.util.Try

trait Database {
  this: DebugEnhancedLogging =>
  import logger._

  private var connection: Connection = _

  val dbDriverClass: String
  val dbUrl: String
  val dbUsername: Option[String]
  val dbPassword: Option[String]

  /**
   * Hook for creating a connection. If a username and password is provided, these will be taken
   * into account when creating the connection; otherwise the connection is created without
   * username and password.
   *
   * @return the connection
   */
  protected def createConnection: Connection = {
    val optConn = for {
      username <- dbUsername
      password <- dbPassword
    } yield DriverManager.getConnection(dbUrl, username, password)

    optConn.getOrElse(DriverManager.getConnection(dbUrl))
  }

  /**
   * Establishes the connection with the database
   */
  def initConnection(): Try[Unit] = Try {
    info("Creating database connection ...")

    Class.forName(dbDriverClass)
    connection = createConnection

    info(s"Database connected with URL = $dbUrl, user = $dbUsername, password = ****")
  }

  /**
   * Close the database's connection.
   *
   * @return `Success` if the closing went well, `Failure` otherwise
   */
  def closeConnection(): Try[Unit] = Try {
    info("Closing database connection ...")
    connection.close()
    info("Database connection closed")
  }

  /**
   * Return the baseId of the given bagId if the latter exists.
   * If the bagId does not exist, a `BagIdNotFoundException` is returned.
   *
   * @param bagId the bagId for which the base bagId needs to be returned
   * @return the baseId of the given bagId if it exists; failure otherwise
   */
  def getBaseBagId(bagId: BagId): Try[BaseId] = {
    trace(bagId)

    val resultSet = for {
      prepStatement <- managed(connection.prepareStatement("SELECT base FROM bag_info WHERE bagId=?;"))
      _ = prepStatement.setString(1, bagId.toString)
      resultSet <- managed(prepStatement.executeQuery())
    } yield resultSet

    resultSet
      .map(result =>
        if (result.next())
          UUID.fromString(result.getString("base"))
        else
          throw BagIdNotFoundException(bagId))
      .tried
  }

  /**
   * Returns a sequence of all bagIds that have the given baseId as their base, ordered by the 'created' timestamp.
   *
   * @param baseId the baseId used during this search
   * @return a sequence of all bagIds with a given baseId
   */
  def getAllBagsWithBase(baseId: BaseId): Try[Seq[BagId]] = {
    trace(baseId)

    val resultSet = for {
      prepStatement <- managed(connection.prepareStatement("SELECT bagId FROM bag_info WHERE base=? ORDER BY created;"))
      _ = prepStatement.setString(1, baseId.toString)
      resultSet <- managed(prepStatement.executeQuery())
    } yield resultSet

    resultSet
      .map(result => Stream.continually(result.next())
        .takeWhile(b => b)
        .map(_ => UUID.fromString(result.getString("bagId")))
        .toList)
      .tried
  }

  /**
   * Returns the `Relation` object for the given bagId if it is present in the database.
   * If the bagId does not exist, a `BagIdNotFoundException` is returned.
   *
   * @param bagId the bagId corresponding to the relation
   * @return the relation data of the given bagId
   */
  def getBagInfo(bagId: BagId): Try[BagInfo] = {
    trace(bagId)

    val resultSet = for {
      prepStatement <- managed(connection.prepareStatement("SELECT * FROM bag_info WHERE bagId=?;"))
      _ = prepStatement.setString(1, bagId.toString)
      resultSet <- managed(prepStatement.executeQuery())
    } yield resultSet

    resultSet
      .map(result =>
        if (result.next())
          BagInfo(
            bagId = UUID.fromString(result.getString("bagId")),
            baseId = UUID.fromString(result.getString("base")),
            created = DateTime.parse(result.getString("created"), dateTimeFormatter))
        else
          throw BagIdNotFoundException(bagId))
      .tried
  }

  /**
   * Returns a sequence of all bag relations that are present in the database.
   * '''Warning:''' this may load large amounts of data into memory.
   *
   * @return a list of all bag relations
   */
  def getAllBagRelations: Try[Seq[BagInfo]] = {
    val resultSet = for {
      statement <- managed(connection.createStatement)
      resultSet <- managed(statement.executeQuery("SELECT * FROM bag_info;"))
    } yield resultSet

    resultSet
      .map(result => Stream.continually(result.next())
        .takeWhile(b => b)
        .map(_ => BagInfo(
          bagId = UUID.fromString(result.getString("bagId")),
          baseId = UUID.fromString(result.getString("base")),
          created = DateTime.parse(result.getString("created"), dateTimeFormatter)))
        .toList)
      .tried
  }

  /**
   * Add a bag relation to the database. A bag relation consists of a unique bagId (that is not yet
   * included in the database), a base bagId and a 'created' timestamp.
   *
   * @param bagId the unique bag identifier
   * @param baseId the base bagId of the bagId
   * @param created the date/time at which the bag was created
   * @return `Success` if the bag relation was added successfully; `Failure` otherwise
   */
  def addBagInfo(bagId: BagId, baseId: BaseId, created: DateTime): Try[Unit] = {
    trace((bagId, baseId, created))

    managed(connection.prepareStatement("INSERT INTO bag_info VALUES (?, ?, ?);"))
      .map(prepStatement => {
        prepStatement.setString(1, bagId.toString)
        prepStatement.setString(2, baseId.toString)
        prepStatement.setString(3, created.toString(dateTimeFormatter))
        prepStatement.executeUpdate()
      })
      .tried
      .map(_ => ())
  }
}
