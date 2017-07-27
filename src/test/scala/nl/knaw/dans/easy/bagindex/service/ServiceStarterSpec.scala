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
package nl.knaw.dans.easy.bagindex.service

import java.nio.file.{ Files, Paths }
import java.sql.DriverManager

import nl.knaw.dans.easy.bagindex.{ ConfigurationSupportFixture, ServerTestSupportFixture, TestSupportFixture }
import org.apache.commons.daemon.DaemonContext
import org.apache.commons.io.FileUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ BeforeAndAfterEach, OneInstancePerTest }
import resource.managed

import scala.io.Source

class ServiceStarterSpec extends TestSupportFixture
  with ServerTestSupportFixture
  with ConfigurationSupportFixture
  with MockFactory
  with BeforeAndAfterEach
  with OneInstancePerTest {

  private lazy val daemon = new ServiceStarter

  private val databaseFile = testDir.resolve("seed.db")
  private val configFile = testDir.resolve("cfg/application.properties")
  private val bagStoreBaseDirs @ Seq(firstBagStore, _@_*) = Seq("first-bag-store", "second-bag-store").map(testDir.resolve)

  override def beforeEach(): Unit = {
    super.beforeEach()

    // set correct configuration parameters
    configuration.properties.setProperty("bag-index.database.url", s"jdbc:sqlite:${ databaseFile.toString }")
    configuration.properties.setProperty("bag-index.bag-store.base-dirs", bagStoreBaseDirs.mkString(","))
    configuration.properties.save(configFile.toFile)
    System.setProperty("app.home", testDir.toString)

    // initialize the database
    managed(DriverManager.getConnection(s"jdbc:sqlite:${ databaseFile.toString }"))
      .flatMap(connection => managed(connection.createStatement))
      .and(managed(Source.fromFile(getClass.getClassLoader.getResource("database/bag-index.sql").toURI)).map(_.mkString))
      .acquireAndGet { case (statement, query) => statement.executeUpdate(query) }

    // initialize the bag stores
    for (bagStore <- bagStoreBaseDirs)
      Files.createDirectory(bagStore)
    val origBagStore = Paths.get(getClass.getClassLoader.getResource("bag-store").toURI)
    FileUtils.deleteDirectory(firstBagStore.toFile)
    FileUtils.copyDirectory(origBagStore.toFile, firstBagStore.toFile)

    // check setup worked as expected
    configFile.toFile should exist
    databaseFile.toFile should exist
    all (bagStoreBaseDirs.map(_.toFile)) should exist

    daemon.init(mock[DaemonContext])
    daemon.start()
  }

  override def afterEach(): Unit = {
    daemon.stop()
    daemon.destroy()
    super.afterEach()
  }

  "calling GET /" should "check that the service is up and running" in {
    callService() shouldBe successful
  }
}
