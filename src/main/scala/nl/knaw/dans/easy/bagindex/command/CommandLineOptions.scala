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
package nl.knaw.dans.easy.bagindex.command

import java.util.UUID

import nl.knaw.dans.easy.bagindex.{ BagId, Version }
import org.apache.commons.configuration.PropertiesConfiguration
import org.joda.time.DateTime
import org.rogach.scallop.{ ScallopConf, ScallopOption, Subcommand, singleArgConverter }

import scala.annotation.tailrec
import scala.io.StdIn

class CommandLineOptions(args: Array[String], properties: PropertiesConfiguration) extends ScallopConf(args) {
  appendDefaultToDescription = true
  editBuilder(_.setHelpWidth(110))

  printedName = "easy-bag-index"
  private val _________ = " " * printedName.length
  version(s"$printedName v${Version()}")
  banner(
    s"""
      |Index for a bag store
      |
      |Usage:
      |
      |$printedName \\
      |${_________}  | index [bagId]
      |
      |Options:
    """.stripMargin)

  private implicit val uuidConverter = singleArgConverter[UUID](UUID.fromString)
  private implicit val dateTimeConverter = singleArgConverter[DateTime](DateTime.parse)

  val index = new Subcommand("index") {
    descr("Adds one bag or the whole bag-store to the index")
    val bagId: ScallopOption[BagId] = trailArg[UUID](name = "bagId",
      descr = "the bag identifier to be added",
      required = false)
  }

  addSubcommand(index)

  footer("")

  object interaction {
    /**
     * Interactive command line interface to ask whether the user is sure to index the whole bag-store
     * and thereby delete all current data in the bag-index.
     *
     * @return `true` if the user wants to proceed, `false` otherwise
     */
    def deleteBeforeIndexing(): Boolean = {
      @tailrec
      def recursiveAsk(): Boolean = {
        StdIn.readLine().toLowerCase match {
          case "yes" => true
          case "no" => false
          case _ =>
            println("either use 'Yes' or 'No'")
            recursiveAsk()
        }
      }

      println("Before the bag-store is indexed, the current index needs to be deleted?")
      println("Do you want to proceed? (Yes/No)")
      recursiveAsk()
    }
  }
}
object CommandLineOptions {
  def apply(args: Array[String], properties: PropertiesConfiguration): CommandLineOptions = new CommandLineOptions(args, properties)
}
