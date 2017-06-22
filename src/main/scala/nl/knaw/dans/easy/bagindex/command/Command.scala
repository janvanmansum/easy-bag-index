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
package nl.knaw.dans.easy.bagindex.command

import nl.knaw.dans.easy.bagindex.BagIndexApp

import scala.language.reflectiveCalls
import scala.util.{ Failure, Success }
import nl.knaw.dans.lib.error._

object Command extends App with BagIndexApp {

  initConnectionPool()

  val opts = CommandLineOptions(args, properties)
  opts.verify()

  // TODO continue with the rest (command parsing etc.)

  doTransaction(implicit connection => {
    opts.subcommand match {
      case Some(cmd @ opts.index) =>
        cmd.bagId.toOption
          .map(addFromBagStore(_).map(_ => s"Added bag with bagId ${ cmd.bagId() }"))
          .getOrElse {
            if (opts.interaction.deleteBeforeIndexing())
              indexBagStore().map(_ => "bag-store index rebuilt successfully.")
            else
              Success("Indexing aborted.")
          }
      case _ => Failure(new IllegalArgumentException(s"Unknown command: ${ opts.subcommand }"))
    }
  })
    .map(msg => println(s"OK: $msg"))
    .doIfFailure { case e => logger.error(e.getMessage, e) }
    .getOrRecover(e => println(s"FAILED: ${ e.getMessage }"))

  closeConnectionPool()
}
