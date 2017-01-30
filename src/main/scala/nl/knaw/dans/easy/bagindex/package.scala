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
package nl.knaw.dans.easy

import java.nio.file.Path
import java.util.{ Optional, Properties, UUID }

import org.joda.time.DateTime
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }

import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

package object bagindex {

  case class BagIdNotFoundException(bagId: BagId) extends Exception(s"The specified bagId ($bagId) does not exist.")
  case class BagNotFoundException(bagDir: Path, cause: Throwable) extends Exception(s"A bag could not be loaded at $bagDir", cause)
  case class BagNotFoundInBagStoreException(bagId: BagId, baseDir: Path) extends Exception(s"The bag with id '$bagId' could not be found in bagstore '${baseDir.toAbsolutePath}'")
  case class InvalidIsVersionOfException(bagId: BagId, value: String) extends Exception(s"Bag with id $bagId has an unsupported value in the bag-info.txt field Is-Version-Of: $value")

  val CONTEXT_ATTRIBUTE_KEY_BAGINDEX_APP = "nl.knaw.dans.easy.bagindex.BagIndexApp"
  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

  type BagId = UUID
  type BaseId = UUID

  // TODO: rename to BagIndexRecord (or something, but BagInfo is easily confused with bag-info.txt)
  case class BagInfo(bagId: BagId, baseId: BaseId, created: DateTime)

  object Version {
    def apply(): String = {
      val props = new Properties()
      props.load(getClass.getResourceAsStream("/Version.properties"))
      props.getProperty("application.version")
    }
  }

  implicit class TryExtensions[T](val t: Try[T]) extends AnyVal {
    // TODO candidate for dans-scala-lib, see also implementation/documentation in easy-split-multi-deposit
    def onError[S >: T](handle: Throwable => S): S = {
      t match {
        case Success(value) => value
        case Failure(throwable) => handle(throwable)
      }
    }
  }

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
}
