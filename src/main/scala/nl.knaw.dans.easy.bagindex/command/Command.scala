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

import nl.knaw.dans.easy.bagindex.service.ServiceWiring
import nl.knaw.dans.lib.error._

import scala.language.reflectiveCalls
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

object Command extends App with CommandLineOptionsComponent with ServiceWiring {

  type FeedBackMessage = String

  override val commandLine: CommandLineOptions = new CommandLineOptions(args) {
    verify()
  }

  val result: Try[FeedBackMessage] = for {
    _ <- databaseAccess.initConnectionPool()
    msg <- runCommandLine
    _ <- databaseAccess.closeConnectionPool()
  } yield msg

  result.doIfSuccess(msg => println(s"OK: $msg"))
    .doIfFailure { case e => logger.error(e.getMessage, e) }
    .doIfFailure { case NonFatal(e) => println(s"FAILED: ${ e.getMessage }") }

  private def runCommandLine: Try[FeedBackMessage] = commandLine.subcommand match {
    case Some(cmd @ commandLine.index) =>
      databaseAccess.doTransaction(implicit connection => {
        cmd.bagId.toOption
          .map(index.addFromBagStore(_).map(_ => s"Added bag with bagId ${ cmd.bagId() }"))
          .getOrElse {
            if (commandLine.interaction.deleteBeforeIndexing())
              indexFull.indexBagStore().map(_ => "bag-store index rebuilt successfully.")
            else
              Success("Indexing aborted.")
          }
      })
    case Some(_ @ commandLine.runService) => runAsService()
    case _ => Failure(new IllegalArgumentException(s"Unknown command: ${ commandLine.subcommand }"))
  }

  private def runAsService(): Try[FeedBackMessage] = Try {
    Runtime.getRuntime.addShutdownHook(new Thread("service-shutdown") {
      override def run(): Unit = {
        logger.info("Stopping service ...")
        server.stop()
        logger.info("Cleaning up ...")
        server.destroy()
        logger.info("Service stopped.")
      }
    })

    server.start()
    logger.info("Service started ...")
    Thread.currentThread.join()
    "Service terminated normally."
  }
}
