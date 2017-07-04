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

import java.nio.file.{ Path, Paths }

import nl.knaw.dans.easy.bagindex.ConfigurationComponent
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import scala.collection.JavaConverters._

trait AccessWiring extends Bagit4FacadeComponent with BagStoreAccessComponent with DatabaseAccessComponent {
  this: ConfigurationComponent with DebugEnhancedLogging =>

  override val bagFacade: BagFacade = new Bagit4Facade()
  override val bagStore: BagStoreAccess = new BagStoreAccess {
    override val baseDirs: Seq[Path] = configuration.properties.getList("bag-index.bag-store.base-dirs")
      .asScala
      .map(dir => Paths.get(dir.asInstanceOf[String]).toAbsolutePath)
  }
  override val databaseAccess: DatabaseAccess = new DatabaseAccess {
    override val dbDriverClassName: String = configuration.properties.getString("bag-index.database.driver-class")
    override val dbUrl: String = configuration.properties.getString("bag-index.database.url")
    override val dbUsername: Option[String] = Option(configuration.properties.getString("bag-index.database.username"))
    override val dbPassword: Option[String] = Option(configuration.properties.getString("bag-index.database.password"))
  }
}
