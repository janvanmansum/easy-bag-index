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
package nl.knaw.dans.easy.bagindex.service

import java.util.UUID

import nl.knaw.dans.easy.bagindex.{ BagIndexApp, _ }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.joda.time.DateTime
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.scalatra._

import scala.util.Try
import scala.xml.PrettyPrinter

case class BagIndexServlet(app: BagIndexApp) extends ScalatraServlet with DebugEnhancedLogging {
  import app._
  get("/") {
    Ok("EASY Bag Index running.")
  }

  // GET: http://bag-index/bag-sequence?contains=<bagId>
  // given a bagId, return a list of bagIds that have the same baseId, ordered by the 'created' timestamp
  // the data is returned as a newline separated (text/plain) String
  get("/bag-sequence") {
    contentType = "text/plain"
    doTransaction(implicit c => {
      Try { UUID.fromString(params("contains")) }
        .flatMap(app.getBagSequence)
    })
      .map(ids => Ok(ids.mkString("\n")))
      .onError(defaultErrorHandling)
  }

  // GET: http://bag-index/bags/<bagId>
  // given a bagId, return the relation data corresponding to this bagId
  // the data is returned as JSON by default or XML when specified (content-type application/xml or text/xml)
  get("/bags/:bagId") {
    doTransaction(implicit c => {
      Try { UUID.fromString(params("bagId")) }
        .flatMap(app.getBagInfo)
        .map(relation =>
          request.getHeader("Accept") match {
            case accept @ ("application/xml" | "text/xml") =>
              contentType = accept
              new PrettyPrinter(80, 4).format {
                // @formatter:off
                <bag-info>
                  <bag-id>{relation.bagId.toString}</bag-id>
                  <base-id>{relation.baseId.toString}</base-id>
                  <created>{relation.created.toString(dateTimeFormatter)}</created>
                </bag-info>
                // @formatter:on
              }
            case _ =>
              contentType = "application/json"
              pretty(render {
                // @formatter:off
                "bag-info" -> {
                  ("bag-id" -> relation.bagId.toString) ~
                  ("base-id" -> relation.baseId.toString) ~
                  ("created" -> relation.created.toString(dateTimeFormatter))
                }
                // @formatter:on
              })
          })
    })
      .map(Ok(_))
      .onError(defaultErrorHandling)
  }

  // PUT: http://bag-index/bags/<bagId>
  // get the bag with the given bagId from the bag-store, read bag-info.txt and get the base and 'created' timestamp properties
  // based on this, add a record to the index/database
  put("/bags/:bagId") {
    doTransaction(implicit connection => {
      Try { UUID.fromString(params("bagId")) }
        .flatMap(addFromBagStore)
    })
      .map(_ => Created())
      .onError(defaultErrorHandling)
  }

  // TODO (low prio) zelfde interface in cmd als in servlet

  private def defaultErrorHandling(t: Throwable): ActionResult = {
    t match {
      case e: IllegalArgumentException => BadRequest(e.getMessage)
      case e: BagIdNotFoundException => NotFound(e.getMessage)
      case e: NotABagDirException => NotFound(e.getMessage)
      case e: BagNotFoundException => NotFound(e.getMessage)
      case e =>
        logger.error("Unexpected type of failure", e)
        InternalServerError(s"[${DateTime.now}] Unexpected type of failure. Please consult the logs")
    }
  }
}
