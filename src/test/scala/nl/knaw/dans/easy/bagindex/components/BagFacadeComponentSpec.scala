package nl.knaw.dans.easy.bagindex.components

import nl.knaw.dans.easy.bagindex.{ BagStoreFixture, Bagit4Fixture, NoDoiFoundException }

import scala.util.{ Failure, Success }
import scala.xml.{ Elem, Node, NodeSeq, XML }
import scala.xml.transform.{ RewriteRule, RuleTransformer }

class BagFacadeComponentSpec extends BagStoreFixture with Bagit4Fixture {

  private val bagStoreBaseDir = baseDirs.headOption.getOrElse(throw new NoSuchElementException("no bagstore base directory found"))

  "getDoi" should "find the DOI identifier in a metadata/dataset.xml file" in {
    bagFacade.getDoi(bagStoreBaseDir.resolve("00/000000000000000000000000000001/bag-revision-1/metadata/dataset.xml")) should matchPattern { case Success("10.17026/dans-2xg-umq8") => }
  }

  it should "fail if the dataset.xml file did not contain a DOI identifier" in {
    val datasetXML = bagStoreBaseDir.resolve("00/000000000000000000000000000001/bag-revision-1/metadata/dataset.xml")

    object RemoveDOI extends RewriteRule {
      override def transform(node: Node): Seq[Node] = node match {
        case Elem(_, "identifier", _, _, _@_*) => NodeSeq.Empty
        case n => n
      }
    }

    new RuleTransformer(RemoveDOI)
      .transform(XML.loadFile(datasetXML.toFile))
      .foreach(XML.save(datasetXML.toString, _))

    bagFacade.getDoi(datasetXML) should matchPattern { case Failure(NoDoiFoundException(`datasetXML`)) => }
  }
}
