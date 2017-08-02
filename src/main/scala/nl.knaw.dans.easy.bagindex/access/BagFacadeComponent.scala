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

import java.io.IOException
import java.net.URI
import java.nio.file.Path
import java.util.Map.Entry
import java.util.UUID

import gov.loc.repository.bagit.domain.Bag
import gov.loc.repository.bagit.exceptions._
import gov.loc.repository.bagit.reader.BagReader
import nl.knaw.dans.easy.bagindex._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Node, XML }
import scala.collection.JavaConverters._

// TODO: (see also: easy-bag-store, easy-archive-bag) Candidate for new library easy-bagit-lib (a facade over the LOC lib)
trait BagFacadeComponent extends DebugEnhancedLogging {

  val bagFacade: BagFacade

  val IS_VERSION_OF = "Is-Version-Of"
  val CREATED = "Created"

  trait BagFacade {
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

    // TODO: canditate for easy-bagit-lib
    private def getIsVersionOfFromUri(bagDir: Path)(uri: URI): Try[UUID] = {
      if(uri.getScheme == "urn") {
        val uuidPart = uri.getSchemeSpecificPart
        val parts = uuidPart.split(':')
        if (parts.length != 2) Failure(InvalidIsVersionOfException(bagDir, uri.toASCIIString))
        else Try { UUID.fromString(parts(1)) }
      } else Failure(InvalidIsVersionOfException(bagDir, uri.toASCIIString))
    }

    def getBagInfo(bagDir: Path): Try[Map[String, String]]

    def getDoi(bagDir: Path): Try[Doi]
  }
}

trait Bagit5FacadeComponent extends BagFacadeComponent with DebugEnhancedLogging {

  class Bagit5Facade(bagReader: BagReader = new BagReader) extends BagFacade {

    private def entryToTuple[K, V](entry: Entry[K, V]): (K, V) = entry.getKey -> entry.getValue

    def getBagInfo(bagDir: Path): Try[Map[String, String]] = {
      trace(bagDir)
      getBag(bagDir).map(_.getMetadata.getAll.asScala.map(entryToTuple).toMap)
    }

    private def getBag(bagDir: Path): Try[Bag] = Try {
      bagReader.read(bagDir)
    }.recoverWith {
      case cause: IOException => Failure(NotABagDirException(bagDir, cause))
      case cause: UnparsableVersionException => Failure(BagReaderException(bagDir, cause))
      case cause: MaliciousPathException => Failure(BagReaderException(bagDir, cause))
      case cause: InvalidBagMetadataException => Failure(BagReaderException(bagDir, cause))
      case cause: UnsupportedAlgorithmException => Failure(BagReaderException(bagDir, cause))
      case cause: InvalidBagitFileFormatException => Failure(BagReaderException(bagDir, cause))
      case NonFatal(cause) => Failure(NotABagDirException(bagDir, cause))
    }

    override def getDoi(datasetXml: Path): Try[Doi] = Try {
      trace(datasetXml)
      val doi = (XML.loadFile(datasetXml.toFile) \ "dcmiMetadata" \ "identifier")
        .find(hasXsiType(NAMESPACE_IDENTIFIER_TYPE, "DOI"))
        .map(node => Success(node.text))
        .getOrElse(Failure(NoDoiFoundException(datasetXml)))

      debug(s"found doi for $datasetXml: $doi")
      doi
    }.flatten

    private val NAMESPACE_SCHEMA_INSTANCE = new URI("http://www.w3.org/2001/XMLSchema-instance")
    private val NAMESPACE_IDENTIFIER_TYPE = new URI("http://easy.dans.knaw.nl/schemas/vocab/identifier-type/")

    // TODO copied from easy-ingest-flow
    private def hasXsiType(attrNamespace: URI, attrValue: String)(e: Node): Boolean = {
      e.attribute(NAMESPACE_SCHEMA_INSTANCE.toString, "type")
        .exists {
          case Seq(n) =>
            n.text.split(":") match {
              case Array(pref, label) => e.getNamespace(pref) == attrNamespace.toString && label == attrValue
              case _ => false
            }
          case _ => false
        }
    }
  }
}
