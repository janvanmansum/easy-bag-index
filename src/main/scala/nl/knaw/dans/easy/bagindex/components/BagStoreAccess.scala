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
package nl.knaw.dans.easy.bagindex.components

import java.nio.file.{ Files, Path }
import java.util.UUID

import nl.knaw.dans.easy.bagindex.JavaOptionals._
import nl.knaw.dans.easy.bagindex.{ BagId, BagNotFoundInBagStoreException }

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }
import scala.collection.JavaConverters._

trait BagStoreAccess {

  val bagStoreBaseDir: Path

  // TODO replace these methods with a call to the BagStore API to retrieve the path of the bag
  // or replace this method with a call to the BagStore API to get the bag-info listing in the bag

  /**
   * Returns the path to a bag in the bagstore identified with the given bagId.
   * If the bag is not found, a `BagNotFoundInBagStoreException` is returned.
   *
   * '''Note:''' this method returns the path up to the actual bag. To get only the path up to
   * the bag's container, use [[toContainer]] instead.
   *
   * @param bagId the bag's bagId
   * @return the path to a bag in the bagstore
   * @see [[toContainer]]
   */
  def toLocation(bagId: BagId): Try[Path] = {
    toContainer(bagId).flatMap(toLocation)
  }

  private def toLocation(container: Path): Try[Path] = Try {
    val containedFiles = Files.list(container).iterator().asScala.toList
    assert(containedFiles.size == 1, s"Corrupt BagStore, container with less or more than one file: $container")
    container.resolve(containedFiles.head)
  }

  /**
   * Return the path to a bag's container in the bagstore identified with the given bagId.
   * If the bag is not found, a `BagNotFoundInBagStoreException` is returned.
   *
   * '''Note:''' this method only returns the path to the bag up to its container. To get the path
   * to the actual bag, use [[toLocation]] instead.
   *
   * @param bagId the bag's bagId
   * @return the path to a bag's container in the bagstore
   */
  def toContainer(bagId: BagId): Try[Path] = {

    @tailrec
    def tailRec(currentPath: Path, restPath: String): Try[Path] = {
      if (restPath.isEmpty)
        Success(currentPath)
      else {
        val res = for {
          subPath <- Files.list(currentPath).findFirst().asScala
          length = subPath.getFileName.toString.length
          (path, restPath2) = restPath.splitAt(length)
          newPath = currentPath.resolve(path)
          if Files.exists(newPath)
        } yield (newPath, restPath2)

        // pattern match is necessary for tail recursion
        res match {
          case Some((path, tail)) => tailRec(path, tail)
          case None => Failure(BagNotFoundInBagStoreException(bagId, bagStoreBaseDir))
        }
      }
    }

    tailRec(bagStoreBaseDir, bagId.toString.filterNot(_ == '-'))
  }

  /**
   * Lists the bagId and path for every bag in the bagstore.
   *
   * @return a stream of `(BagId, Path)` tuples
   */
  def traverse: Try[Stream[(BagId, Path)]] = {

    // we assume all bags in a bagstore are at equal depth, so following one path is enough!
    def probeForPathDepth: Try[Int] = Try {

      @tailrec
      def probe(path: Path, length: Int, levels: Int): Int = {
        length match {
          case l if l > 0 =>
            val p = Files.list(path).findFirst().get
            probe(p, length - p.getFileName.toString.length, levels + 1)
          case 0 => levels
          case _ => throw new Exception("corrupt bagstore")
        }
      }

      probe(bagStoreBaseDir, UUID.randomUUID().toString.filterNot(_ == '-').length, 0)
    }

    def formatUuidStrCanonically(s: String): String = {
      List(s.slice(0, 8), s.slice(8, 12), s.slice(12, 16), s.slice(16, 20), s.slice(20, 32)).mkString("-")
    }

    def traverse(depth: Int): Try[Stream[(BagId, Path)]] = Try {
      Files.walk(bagStoreBaseDir, depth).iterator().asScala.toStream
        .map(bagStoreBaseDir.relativize)
        .withFilter(_.getNameCount == depth)
        .map(path => {
          val bagId = UUID.fromString(formatUuidStrCanonically(path.toString.filterNot(_ == '/')))
          (bagId, toLocation(bagStoreBaseDir.resolve(path)).get)
        })
    }

    for {
      depth <- probeForPathDepth
      bags <- traverse(depth)
    } yield bags
  }
}
