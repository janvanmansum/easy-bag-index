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

import java.net.URI
import java.nio.file.{Files, Path}
import java.util.UUID

import nl.knaw.dans.easy.bagindex.JavaOptionals._
import nl.knaw.dans.easy.bagindex.{BagId, BagNotFoundInBagStoreException, BaseId, InvalidIsVersionOfException, dateTimeFormatter}
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait AddBagFromBagStore {
  this: AddBagToIndex
    with BagStoreAccess
    with BagFacadeComponent
    with DebugEnhancedLogging =>

  val IS_VERSION_OF = "Is-Version-Of"
  val CREATED = "Created"

  /**
   * Add the bag info of the bag identified with bagId to the database.
   * Specificly, the 'Is-Version-Of' and 'Created' fields from the bag's `bag-info.txt` are read
   * and added to the database. If the 'Is-Version-Of' points to a bag that is not a base bag,
   * the ''real'' base bag is related with this bagId instead.
   *
   * @param bagId the bagId identifying the bag to be indexed
   * @return the baseId that linked with the given bagId
   */
  def addFromBagStore(bagId: BagId): Try[BaseId] = {
    trace(bagId)
    for {
      bagDir <- toLocation(bagId)
      bagInfo <- bagFacade.getBagInfo(bagDir)
      baseId <- bagInfo.get(IS_VERSION_OF)
        .map(s => Try {
          new URI(s)
        }.flatMap(getIsVersionOfFromUri(bagId)).map(Option(_)))
        .getOrElse(Success(None))
      created = bagInfo.get(CREATED).map(DateTime.parse(_, dateTimeFormatter))
      superBaseId <- baseId.map(add(bagId, _, created)).getOrElse(addBase(bagId, created))
    } yield superBaseId
  }

  // TODO: canditate for easy-bagit-lib
  private def getIsVersionOfFromUri(bagId: BagId)(uri: URI): Try[UUID] = {
    if(uri.getScheme == "urn") {
      val uuidPart = uri.getSchemeSpecificPart
      val parts = uuidPart.split(':')
      if (parts.length != 2) Failure(InvalidIsVersionOfException(bagId, uri.toASCIIString))
      else Try { UUID.fromString(parts(1)) }
    } else Failure(InvalidIsVersionOfException(bagId, uri.toASCIIString))
  }

  // TODO replace this method with a call to the BagStore API to retrieve the path of the bag
  // or replace this method with a call to the BagStore API to get the bag-info listing in the bag
  private def toLocation(bagId: BagId): Try[Path] = {
    toContainer(bagId)
      .map(container => Try {
        val containedFiles = Files.list(container).iterator().asScala.toList
        assert(containedFiles.size == 1, s"Corrupt BagStore, container with less or more than one file: $container")
        container.resolve(containedFiles.head)
      })
      .getOrElse(Failure(BagNotFoundInBagStoreException(bagId, bagStoreBaseDir)))
  }

  private def toContainer(bagId: BagId): Option[Path] = {

    @tailrec
    def tailRec(currentPath: Path, restPath: String): Option[Path] = {
      if (restPath.isEmpty)
        Option(currentPath)
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
          case None => None
        }
      }
    }

    tailRec(bagStoreBaseDir, bagId.toString.filterNot(_ == '-'))
  }
}
