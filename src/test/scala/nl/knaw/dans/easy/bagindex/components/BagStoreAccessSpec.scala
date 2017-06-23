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
package nl.knaw.dans.easy.bagindex.components

import java.nio.file.Files
import java.util.UUID

import nl.knaw.dans.easy.bagindex.{ BagNotFoundException, BagStoreFixture }
import org.apache.commons.io.FileUtils

import scala.util.{ Failure, Success }

class BagStoreAccessSpec extends BagStoreFixture {
  private val bagStoreBaseDir = baseDirs.headOption.getOrElse(throw new NoSuchElementException("no bagstore base directory found"))

  "toLocation" should "resolve the path to the actual bag identified with a bagId" in {
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    inside(toLocation(bagId)) {
      case Success(path) => path shouldBe bagStoreBaseDir.resolve("00/000000000000000000000000000001/bag-revision-1")
    }
  }

  it should "fail with a BagNotFoundInBagStoreException when the bag is not in the bagstore" in {
    val bagId = UUID.randomUUID()
    toLocation(bagId) should matchPattern { case Failure(BagNotFoundException(`bagId`)) => }
  }

  "toContainer" should "resolve the path to the bag's container identified with a bagId" in {
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val container = bagStoreBaseDir.resolve("00/000000000000000000000000000001")

    toContainer(bagId, bagStoreBaseDir) should matchPattern { case Success(`container`) => }
  }

  it should "return a None if the bag is not in the bagstore" in {
    val bagId = UUID.randomUUID()
    toContainer(bagId, bagStoreBaseDir) should matchPattern { case Failure(BagNotFoundException(`bagId`)) => }
  }

  "toDatasetXml" should "resolve the path to the metadata/dataset.xml file of the bag identifier with a bagId" in {
    val bagId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    toDatasetXml(bagStoreBaseDir.resolve("00/000000000000000000000000000001/bag-revision-1/"), bagId) shouldBe
      bagStoreBaseDir.resolve("00/000000000000000000000000000001/bag-revision-1/metadata/dataset.xml")
  }

  "traverse" should "list all bags in the bagstore" in {
    val uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val uuid3 = UUID.fromString("00000000-0000-0000-0000-000000000003")

    val path1 = bagStoreBaseDir.resolve("00/000000000000000000000000000001/bag-revision-1")
    val path2 = bagStoreBaseDir.resolve("00/000000000000000000000000000002/bag-revision-2")
    val path3 = bagStoreBaseDir.resolve("00/000000000000000000000000000003/bag-revision-3")

    inside(traverse) {
      case Success(stream) => stream.toList should (have size 3 and
        contain only((uuid1, path1), (uuid2, path2), (uuid3, path3)))
    }
  }

  it should "return an empty collection if the bagstore is empty" in {
    // create an empty bagstore
    FileUtils.deleteDirectory(bagStoreBaseDir.toFile)
    Files.createDirectory(bagStoreBaseDir)

    inside(traverse) {
      case Success(stream) => stream.toList shouldBe empty
    }
  }
}
