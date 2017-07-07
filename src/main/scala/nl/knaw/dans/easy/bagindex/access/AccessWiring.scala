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
package nl.knaw.dans.easy.bagindex.access

import java.nio.file.{ Files, Path, Paths }

import nl.knaw.dans.easy.bagindex.ConfigurationComponent

import scala.collection.JavaConverters._

trait AccessWiring extends Bagit4FacadeComponent
  with BagStoreAccessComponent
  with DatabaseAccessComponent {
  this: ConfigurationComponent =>

  override lazy val bagFacade: BagFacade = new Bagit4Facade()
  override lazy val bagStore: BagStoreAccess = new BagStoreAccess {
    override val baseDirs: Seq[Path] = configuration.properties.getList("bag-index.bag-store.base-dirs")
      .asScala
      .map(dir => Paths.get(dir.asInstanceOf[String]).toAbsolutePath)

    baseDirs.collect { case base if !Files.isReadable(base) => base } match {
      case Seq() => true // everything ok!
      case xs => throw new IllegalArgumentException(s"Non-existing or non-readable bag-store(s): ${xs.mkString(", ")}")
    }
  }
  override lazy val databaseAccess: DatabaseAccess = new DatabaseAccess {
    override val dbDriverClassName: String = configuration.properties.getString("bag-index.database.driver-class")
    override val dbUrl: String = configuration.properties.getString("bag-index.database.url")
    override val dbUsername: Option[String] = Option(configuration.properties.getString("bag-index.database.username"))
    override val dbPassword: Option[String] = Option(configuration.properties.getString("bag-index.database.password"))

    private def usernamePasswordSettings = {
      (dbUsername, dbPassword) match {
        case (Some(_), Some(_)) | (None, None) => true
        case _ => false
      }
    }
    require(usernamePasswordSettings, "database username and password should be either both defined or not defined")
  }
}
