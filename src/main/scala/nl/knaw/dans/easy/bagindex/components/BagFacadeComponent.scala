package nl.knaw.dans.easy.bagindex.components

import java.nio.file.Path

import gov.loc.repository.bagit.{ Bag, BagFactory }
import nl.knaw.dans.easy.bagindex.BagNotFoundException

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.{ Failure, Try }

trait BagFacadeComponent {

  val bagFacade: BagFacade

  trait BagFacade {
    def getBagInfo(bagDir: Path): Try[Map[String, String]]
  }
}

trait Bagit4FacadeComponent extends BagFacadeComponent {
  class Bagit4Facade(bagFactory: BagFactory = new BagFactory) extends BagFacade {

    def getBagInfo(bagDir: Path): Try[Map[String, String]] = {
      for {
        bag <- getBag(bagDir)
      } yield bag.getBagInfoTxt.asScala.toMap
    }

    private def getBag(bagDir: Path): Try[Bag] = Try {
      bagFactory.createBag(bagDir.toFile, BagFactory.Version.V0_97, BagFactory.LoadOption.BY_MANIFESTS)
    }.recoverWith { case cause => Failure(BagNotFoundException(bagDir, cause)) }
  }
}
