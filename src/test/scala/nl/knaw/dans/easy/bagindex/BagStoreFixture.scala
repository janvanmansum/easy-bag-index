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

import java.nio.file.{ Path, Paths }
import java.util.UUID

import nl.knaw.dans.easy.bagindex.access.BagStoreAccessComponent
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach

trait BagStoreFixture extends BagStoreAccessComponent {
  this: TestSupportFixture =>

  def initBagStores: Path = {
    val bagStoreBaseDir = testDir.resolve("bag-store")
    val origBagStore = Paths.get("src/test/resources/bag-store")
    FileUtils.deleteDirectory(bagStoreBaseDir.toFile)
    FileUtils.copyDirectory(origBagStore.toFile, bagStoreBaseDir.toFile)
    bagStoreBaseDir
  }

  override val bagStore = new BagStoreAccess {
    val baseDirs: Seq[Path] = Seq(initBagStores)
  }

  /**
   * these are the UUID -> DOI mappings from the bag-store in `test/resources/bag-store`
   */
  lazy val doiMap = Map(
    UUID.fromString("00000000-0000-0000-0000-000000000001") -> "10.5072/dans-2xg-umq8",
    UUID.fromString("00000000-0000-0000-0000-000000000002") -> "10.5072/dans-2xg-umq9",
    UUID.fromString("00000000-0000-0000-0000-000000000003") -> "10.5072/dans-2xg-umq0"
  )
  def doiMap(uuidString: String): String = doiMap(UUID.fromString(uuidString))
}
