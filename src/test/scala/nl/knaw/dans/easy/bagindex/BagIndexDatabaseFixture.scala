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
package nl.knaw.dans.easy.bagindex

import java.sql.Connection

import nl.knaw.dans.easy.bagindex.components.{ Database, DatabaseAccess }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.scalatest.BeforeAndAfter

import scala.io.Source
import resource._

import scala.util.Success

trait BagIndexDatabaseFixture extends TestSupportFixture
  with BeforeAndAfter
  with DatabaseAccess
  with Database
  with DebugEnhancedLogging {

  override val dbDriverClassName: String = "org.sqlite.JDBC"
  override val dbUrl: String = s"jdbc:sqlite:${testDir.resolve("database.db").toString}"
  override val dbUsername = Option.empty[String]
  override val dbPassword = Option.empty[String]

  implicit var connection: Connection = _

  override protected def createConnectionPool: ConnectionPool = {
    val pool = super.createConnectionPool

    managed(pool.getConnection)
      .flatMap(connection => managed(connection.createStatement))
      .and(managed(Source.fromFile(getClass.getClassLoader.getResource("database/bag-index.sql").toURI)).map(_.mkString))
      .map { case (statement, query) =>
        statement.executeUpdate(query)
      }
      .tried shouldBe a[Success[_]]

    connection = pool.getConnection

    pool
  }

  before {
    initConnectionPool()
  }

  after {
    connection.close()
    closeConnectionPool()
  }
}
