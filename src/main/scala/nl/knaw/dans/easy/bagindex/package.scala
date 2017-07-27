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
package nl.knaw.dans.easy

import java.nio.file.Path
import java.util.{ Optional, Properties, UUID }

import org.joda.time.DateTime
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }

import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

package object bagindex {

  case class BagIdNotFoundException(bagId: BagId) extends Exception(s"The specified bagId ($bagId) does not exist")
  case class NotABagDirException(bagDir: Path, cause: Throwable) extends Exception(s"A bag could not be loaded at $bagDir", cause)
  case class BagNotFoundException(bagId: BagId) extends Exception(s"The bag with id '$bagId' could not be found")
  case class NoBagInfoFoundException(bagDir: Path) extends Exception(s"The bag at '$bagDir' does not have a file 'bag-info.txt'")
  case class InvalidIsVersionOfException(bagDir: Path, value: String) extends Exception(s"Bag at '$bagDir' has an unsupported value in the bag-info.txt for field Is-Version-Of: $value")
  case class NoDoiFoundException(datasetXML: Path) extends Exception(s"The metadata/dataset.xml at '$datasetXML' does not contain a DOI identifier")
  case class BagAlreadyInIndexException(bagId: BagId) extends Exception(s"Bag '$bagId' is already in the index")

  val CONTEXT_ATTRIBUTE_KEY_BAGINDEX_APP = "nl.knaw.dans.easy.bagindex.BagIndexApp"
  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

  type BagId = UUID
  type BaseId = UUID
  type Doi = String

  // TODO: rename to BagIndexRecord (or something, but BagInfo is easily confused with bag-info.txt)
  case class BagInfo(bagId: BagId, baseId: BaseId, created: DateTime, doi: Doi)

  /**
   * Conversions between Scala Option and Java 8 Optional.
   */
  object JavaOptionals {
    implicit def toRichOptional[T](optional: Optional[T]): RichOptional[T] = new RichOptional[T](optional)
  }

  class RichOptional[T] (opt: Optional[T]) {

    /**
     * Transform this Optional to an equivalent Scala Option
     */
    def asScala: Option[T] = if (opt.isPresent) Some(opt.get()) else None
  }

  // TODO: will NOT be in dans-scala-lib, because it actually doesn't work correctly...
  implicit class FailFastStream[T](val stream: Stream[Try[T]]) {
    def failFast: Try[Stream[T]] = {
      stream.find(_.isFailure)
        .map(_.flatMap(s => Failure(new IllegalArgumentException(s"Success should never occur here, but got Success($s)"))))
        .getOrElse(Success(stream.map(_.get)))
    }
  }
}
