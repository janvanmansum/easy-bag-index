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

import java.net.URI
import java.nio.file.Path
import java.util.UUID

import gov.loc.repository.bagit.{ Bag, BagFactory }
import nl.knaw.dans.easy.bagindex._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Node, XML }

// TODO: (see also: easy-bag-store, easy-archive-bag) Candidate for new library easy-bagit-lib (a facade over the LOC lib)
trait BagFacadeComponent {

  val bagFacade: BagFacade

  val IS_VERSION_OF = "Is-Version-Of"
  val CREATED = "Created"

  trait BagFacade {
    def getIndexRelevantBagInfo(bagDir: Path): Try[(Option[BaseId], Option[DateTime])]

    def getBagInfo(bagDir: Path): Try[Map[String, String]]

    def getDoi(bagDir: Path): Try[Doi]
  }
}

trait Bagit4FacadeComponent extends BagFacadeComponent with DebugEnhancedLogging {

  class Bagit4Facade(bagFactory: BagFactory = new BagFactory) extends BagFacade {

    def getIndexRelevantBagInfo(bagDir: Path): Try[(Option[BaseId], Option[DateTime])] = {
      trace(bagDir)
      for {
        info <- getBagInfo(bagDir)
        baseId <- info.get(IS_VERSION_OF)
          .map(ivo => Try(new URI(ivo)).flatMap(getIsVersionOfFromUri(bagDir)).map(Option(_)))
          .getOrElse(Success(None))
        created = info.get(CREATED).map(DateTime.parse(_, dateTimeFormatter))
        _ = debug(s"found baseId for $bagDir: $baseId")
        _ = debug(s"  corresponding CREATED date: ${ created.fold("None")(_.toString(dateTimeFormatter)) }")
      } yield (baseId, created)
    }

    def getBagInfo(bagDir: Path): Try[Map[String, String]] = {
      trace(bagDir)
      for {
        bag <- getBag(bagDir)
        info <- Option(bag.getBagInfoTxt) // this call returns null if there is not bag-info.txt
          .map(map => Success(map.asScala.toMap))
          .getOrElse(Failure(NoBagInfoFoundException(bagDir)))
      } yield info
    }

    private def getBag(bagDir: Path): Try[Bag] = Try {
      bagFactory.createBag(bagDir.toFile, BagFactory.Version.V0_97, BagFactory.LoadOption.BY_MANIFESTS)
    }.recoverWith { case cause => Failure(NotABagDirException(bagDir, cause)) }

    // TODO: canditate for easy-bagit-lib
    private def getIsVersionOfFromUri(bagDir: Path)(uri: URI): Try[UUID] = {
      if(uri.getScheme == "urn") {
        val uuidPart = uri.getSchemeSpecificPart
        val parts = uuidPart.split(':')
        if (parts.length != 2) Failure(InvalidIsVersionOfException(bagDir, uri.toASCIIString))
        else Try { UUID.fromString(parts(1)) }
      } else Failure(InvalidIsVersionOfException(bagDir, uri.toASCIIString))
    }

    def getDoi(datasetXML: Path): Try[Doi] = Try {
      trace(datasetXML)
      val doi = (XML.loadFile(datasetXML.toFile) \ "dcmiMetadata" \ "identifier")
        .find(hasXsiType(NAMESPACE_IDENTIFIER_TYPE, "DOI"))
        .map(node => Success(node.text))
        .getOrElse(Failure(NoDoiFoundException(datasetXML)))

      debug(s"found doi for $datasetXML: $doi")
      doi
    }.flatten

    private val NAMESPACE_SCHEMA_INSTANCE = new URI("http://www.w3.org/2001/XMLSchema-instance")
    private val NAMESPACE_IDENTIFIER_TYPE = new URI("http://easy.dans.knaw.nl/schemas/vocab/identifier-type/")

    // TODO copied from easy-ingest-flow
    private def hasXsiType(attrNamespace: URI, attrValue: String)(e: Node): Boolean = {
      e.head
        .attribute(NAMESPACE_SCHEMA_INSTANCE.toString, "type")
        .exists {
          case Seq(n) =>
            val split = n.text.lastIndexOf(':')
            if (split == -1 || split == n.text.length - 1) false
            else {
              val pref = n.text.substring(0, split)
              val label = n.text.substring(split + 1)
              e.head.getNamespace(pref) == attrNamespace.toString && label == attrValue
            }
          case _ => false
        }
    }
  }
}
