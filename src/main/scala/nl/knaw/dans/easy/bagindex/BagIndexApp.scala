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

import java.io.File
import java.nio.file.{ Files, Path, Paths }

import nl.knaw.dans.easy.bagindex.components._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration

trait BagIndexApp extends AddBagToIndex
  with GetBagFromIndex
  with AddBagFromBagStore
  with IndexBagStore
  with DatabaseAccess
  with Database
  with IndexBagStoreDatabase
  with BagStoreAccess
  with Bagit4FacadeComponent
  with DebugEnhancedLogging {

  val properties = new PropertiesConfiguration(new File(System.getProperty("app.home"), "cfg/application.properties"))

  val dbDriverClass: String = properties.getString("bag-index.database.driver-class")
  val dbUrl: String = properties.getString("bag-index.database.url")
  val dbUsername: Option[String] = Option(properties.getString("bag-index.database.username"))
  val dbPassword: Option[String] = Option(properties.getString("bag-index.database.password"))
  val bagStoreBaseDir: Path = Paths.get(properties.getString("bag-index.bag-store.base-dir")).toAbsolutePath
  val bagFacade = new Bagit4Facade()

  def validateSettings(): Unit = {
    def userPasswordSettings = {
      dbUsername.isEmpty && dbPassword.isEmpty || dbUsername.isDefined && dbPassword.isDefined
    }
    assert(userPasswordSettings, "database username and password should be either both defined or not defined")
    assert(Files.isReadable(bagStoreBaseDir), s"Non-existing or non-readable bag-store: $bagStoreBaseDir")
  }
}
