package nl.knaw.dans.easy.bagindex.components

import java.nio.file.{ Files, Path }
import java.util.UUID

import nl.knaw.dans.easy.bagindex.JavaOptionals._
import nl.knaw.dans.easy.bagindex.{ BagId, BagNotFoundInBagStoreException, BaseId, dateTimeFormatter }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{ Failure, Try }

trait AddBagFromBagStore {
  this: AddBagToIndex with BagStoreAccess with BagFacadeComponent with DebugEnhancedLogging =>

  val IS_VERSION_OF = "Is-Version-Of"
  val CREATED = "Created"

  def addFromBagStore(bagId: BagId): Try[BaseId] = {
    trace(bagId)
    for {
      bagDir <- toLocation(bagId)
      bagInfo <- bagFacade.getBagInfo(bagDir)
      baseId = bagInfo.get(IS_VERSION_OF).map(UUID.fromString)
      created = bagInfo.get(CREATED).map(DateTime.parse(_, dateTimeFormatter))
      superBaseId <- baseId.map(add(bagId, _, created)).getOrElse(addBase(bagId, created))
    } yield superBaseId
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

        res match {
          case Some((path, tail)) => tailRec(path, tail)
          case None => None
        }
      }
    }

    tailRec(bagStoreBaseDir, bagId.toString.filterNot(_ == '-'))
  }
}
