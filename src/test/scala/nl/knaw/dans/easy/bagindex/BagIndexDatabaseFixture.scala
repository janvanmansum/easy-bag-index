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

trait BagIndexDatabaseFixture extends TestSupportFixture
  with BeforeAndAfter
  with DatabaseAccess
  with Database
  with DebugEnhancedLogging {

  val dbDriverClass: String = "org.sqlite.JDBC"
  val dbUrl: String = "jdbc:sqlite::memory:"
  val dbUsername = Option.empty[String]
  val dbPassword = Option.empty[String]

  override protected def createConnection: Connection = {
    val con = super.createConnection

    val query = Source.fromFile(getClass.getClassLoader.getResource("database/bag-index.sql").toURI).mkString
    debug(s"Executing query: $query")
    val statement = con.createStatement
    debug("Statement created.")
    statement.executeUpdate(query)
    debug("Statement executed.")
    statement.close()

    con
  }

  before {
    initConnection()
  }

  after {
    closeConnection()
  }
}
