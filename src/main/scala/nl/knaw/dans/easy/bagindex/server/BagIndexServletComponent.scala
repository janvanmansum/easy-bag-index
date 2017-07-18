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
package nl.knaw.dans.easy.bagindex.server

import java.util.UUID

import nl.knaw.dans.easy.bagindex._
import nl.knaw.dans.easy.bagindex.access.DatabaseAccessComponent
import nl.knaw.dans.easy.bagindex.components.{ DatabaseComponent, IndexBagComponent }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime
import org.json4s.JValue
import org.json4s.JsonAST.JArray
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.scalatra._

import scala.util.{ Failure, Success, Try }
import scala.xml.{ Node, PrettyPrinter }

trait BagIndexServletComponent {
  this: IndexBagComponent with DatabaseComponent with DatabaseAccessComponent =>

  val bagIndexServlet: BagIndexServlet

  trait BagIndexServlet extends ScalatraServlet with DebugEnhancedLogging {

    private def toXml(bagInfo: BagInfo): Node = {
    <bag-info>
      <bag-id>{bagInfo.bagId.toString}</bag-id>
      <base-id>{bagInfo.baseId.toString}</base-id>
      <created>{bagInfo.created.toString(dateTimeFormatter)}</created>
      <doi>{bagInfo.doi}</doi>
    </bag-info>
  }

    private def toJson(bagInfo: BagInfo): JValue = {
      "bag-info" -> {
        ("bag-id" -> bagInfo.bagId.toString) ~
          ("base-id" -> bagInfo.baseId.toString) ~
          ("created" -> bagInfo.created.toString(dateTimeFormatter)) ~
          ("doi" -> bagInfo.doi)
      }
    }

    private def createResponse[T](toXml: T => Node)(toJson: T => JValue): T => String = {
      request.getHeader("Accept") match {
        case accept @ ("application/xml" | "text/xml") =>
          contentType = accept
          (new PrettyPrinter(80, 4).format(_: Node)) compose toXml
        case _ =>
          contentType = "application/json"
          pretty _ compose (render _ compose toJson)
      }
    }

    get("/") {
      Ok("EASY Bag Index running.")
    }

    get("/search") {
      def searchWithDoi(doi: Doi) = {
        databaseAccess.doTransaction(implicit c => {
          database.getBagsWithDoi(doi)
            .map(createResponse[Seq[BagInfo]](relations => <result>{relations.map(toXml)}</result>)(relations => "result" -> relations.map(toJson)))
        })
      }

      Option(params)
        .filter(_.nonEmpty)
        .map(params => {
          lazy val doi = params.get("doi").map(searchWithDoi)
          doi
            // other searches added here with .orElse
            .getOrElse(Failure(new IllegalArgumentException("query parameter not supported")))
        })
        .getOrElse(Failure(new IllegalArgumentException("no search query specified")))
        .map(Ok(_))
        .getOrRecover(defaultErrorHandling)
    }

    // GET: http://bag-index/bag-sequence?contains=<bagId>
    // given a bagId, return a list of bagIds that have the same baseId, ordered by the 'created' timestamp
    // the data is returned as a newline separated (text/plain) String
    get("/bag-sequence") {
      contentType = "text/plain"
      params.get("contains")
        .map(uuidStr => {
          Try { UUID.fromString(uuidStr) }
            .recoverWith {
              case _: IllegalArgumentException => Failure(new IllegalArgumentException(s"invalid UUID string: $uuidStr"))
            }
            .flatMap(uuid => databaseAccess.doTransaction(implicit c => index.getBagSequence(uuid)))
            .recoverWith {
              case BagIdNotFoundException(_) => Success(List.empty)
            }
        })
        .getOrElse(Failure(new IllegalArgumentException("query parameter 'contains' not found")))
        .map(ids => Ok(ids.mkString("\n")))
        .getOrRecover(defaultErrorHandling)
    }

    // GET: http://bag-index/bags/<bagId>
    // given a bagId, return the relation data corresponding to this bagId
    // the data is returned as JSON by default or XML when specified (content-type application/xml or text/xml)
    get("/bags/:bagId") {
      val uuidStr = params("bagId")
      Try { UUID.fromString(uuidStr) }
        .recoverWith {
          case _: IllegalArgumentException => Failure(new IllegalArgumentException(s"invalid UUID string: $uuidStr"))
        }
        .flatMap(uuid => databaseAccess.doTransaction(implicit c => database.getBagInfo(uuid)))
        .map(createResponse[BagInfo](bagInfo => <result>{toXml(bagInfo)}</result>)(bagInfo => "result" -> toJson(bagInfo)))
        .recoverWith {
          case BagIdNotFoundException(_) => Success(createResponse[Unit](_ => <result/>)(_ => "result" -> JArray(List.empty))(()))
        }
        .map(Ok(_))
        .getOrRecover(defaultErrorHandling)
    }

    // PUT: http://bag-index/bags/<bagId>
    // get the bag with the given bagId from the bag-store, read bag-info.txt and get the base and 'created' timestamp properties
    // based on this, add a record to the index/database
    put("/bags/:bagId") {
      val uuidStr = params("bagId")
      Try { UUID.fromString(uuidStr) }
        .recoverWith {
          case _: IllegalArgumentException => Failure(new IllegalArgumentException(s"invalid UUID string: $uuidStr"))
        }
        .flatMap(uuid => databaseAccess.doTransaction(implicit c => index.addFromBagStore(uuid)))
        .map(_ => Created())
        .getOrRecover(defaultErrorHandling)
    }

    // TODO (low prio) zelfde interface in cmd als in servlet

    private def defaultErrorHandling(t: Throwable): ActionResult = {
      t match {
        case e: IllegalArgumentException => BadRequest(e.getMessage)
        case e: BagReaderException => BadRequest(e.getMessage)
        case e: BagIdNotFoundException => NotFound(e.getMessage)
        case e: NotABagDirException => NotFound(e.getMessage)
        case e: InvalidIsVersionOfException => BadRequest(e.getMessage)
        case e: BagNotFoundException => BadRequest(e.getMessage)
        case e: NoDoiFoundException => BadRequest(e.getMessage)
        case e: BagAlreadyInIndexException => BadRequest(e.getMessage)
        case e =>
          logger.error("Unexpected type of failure", e)
          InternalServerError(s"[${ DateTime.now }] Unexpected type of failure. Please consult the logs")
      }
    }
  }
}
