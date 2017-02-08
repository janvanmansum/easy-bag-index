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

import java.sql.{ Connection, SQLException }

import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.easy.bagindex._
import org.apache.commons.dbcp2.BasicDataSource
import resource._

import scala.util.control.NonFatal
import scala.util.{ Failure, Try }

trait DatabaseAccess {
  this: DebugEnhancedLogging =>
  import logger._

  private var pool: BasicDataSource = _

  val dbDriverClassName: String
  val dbUrl: String
  val dbUsername: Option[String]
  val dbPassword: Option[String]

  protected def createConnectionPool: BasicDataSource = {
    val source = new BasicDataSource
    source.setDriverClassName(dbDriverClassName)
    source.setUrl(dbUrl)
    dbUsername.foreach(source.setUsername)
    dbPassword.foreach(source.setPassword)

    source
  }

  def initConnectionPool(): Try[Unit] = Try {
    info("Creating database connection ...")
    pool = createConnectionPool
    info(s"Database connected with URL = $dbUrl, user = $dbUsername, password = ****")
  }

  def closeConnectionPool(): Try[Unit] = Try {
    info("Closing database connection ...")
    pool.close()
    info("Database connection closed")
  }

  // TODO test and document
  def doTransaction[T](f: Connection => Try[T]): Try[T] = {
    managed(pool.getConnection)
      .map(connection => {
        connection.setAutoCommit(false)
        val savepoint = connection.setSavepoint()

        f(connection)
          .ifSuccess(_ => {
            connection.commit()
            connection.setAutoCommit(true)
          })
          .recoverWith {
            case NonFatal(e) => Try { connection.rollback(savepoint) }.flatMap(_ => Failure(e))
          }
      })
      .tried
      .flatten
  }
}
