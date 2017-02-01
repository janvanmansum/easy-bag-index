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

import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

trait DatabaseAccess {
  this: DebugEnhancedLogging =>
  import logger._

  protected var connection: Connection = _

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
}
