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
package nl.knaw.dans.easy.bagindex

import java.nio.file.Path
import java.sql.Connection

import nl.knaw.dans.easy.bagindex.access.DatabaseAccessComponent
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach
import resource._

import scala.io.Source

trait BagIndexDatabaseFixture extends BeforeAndAfterEach with DatabaseAccessComponent {
  this: TestSupportFixture =>

  private val databaseDir: Path = testDir.resolve("database")

  implicit var connection: Connection = _

  override val databaseAccess: DatabaseAccess = new DatabaseAccess {
    override val dbDriverClassName: String = "org.hsqldb.jdbcDriver"
    override val dbUrl: String = s"jdbc:hsqldb:file:${ databaseDir.toString }/db"
    override val dbUsername = Option.empty[String]
    override val dbPassword = Option.empty[String]

    override protected def createConnectionPool: ConnectionPool = {
      val pool = super.createConnectionPool

      managed(pool.getConnection)
        .flatMap(connection => managed(connection.createStatement))
        .and(managed(Source.fromFile(getClass.getClassLoader.getResource("database/bag-index.sql").toURI)).map(_.mkString))
        .acquireAndGet { case (statement, query) => statement.executeUpdate(query) }

      connection = pool.getConnection

      pool
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    FileUtils.deleteQuietly(databaseDir.toFile)
    databaseAccess.initConnectionPool()
  }

  override def afterEach(): Unit = {
    managed(connection.createStatement).acquireAndGet(_.execute("SHUTDOWN"))
    connection.close()
    databaseAccess.closeConnectionPool()
    super.afterEach()
  }
}
