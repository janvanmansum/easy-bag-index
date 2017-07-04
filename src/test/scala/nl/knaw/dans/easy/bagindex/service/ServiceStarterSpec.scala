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

import nl.knaw.dans.easy.bagindex.{ BagIndexDatabaseFixture, BagStoreFixture, Bagit4Fixture, ConfigurationSupportFixture, ServerTestSupportFixture }
import org.scalatest.OneInstancePerTest

class ServiceStarterSpec extends BagIndexDatabaseFixture with Bagit4Fixture with BagStoreFixture with ServerTestSupportFixture with ConfigurationSupportFixture with OneInstancePerTest {

  private lazy val daemon = new ServiceStarter
  private lazy val database = daemon.service.database

  override def beforeEach(): Unit = {
    super.beforeEach()

    configuration.properties.setProperty("bag-index.database.url", s"jdbc:sqlite:${ databaseFile.toString }")
    configuration.properties.save(testDir.resolve("cfg/application.properties").toFile)
    System.setProperty("app.home", testDir.toString)

    daemon.init(null)
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
