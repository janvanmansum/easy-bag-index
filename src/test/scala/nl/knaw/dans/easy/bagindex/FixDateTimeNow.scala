/**
 * Copyright (C) 2018 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.easy.bagindex

import org.joda.time.{ DateTime, DateTimeUtils }
import org.scalatest.BeforeAndAfterEach

trait FixDateTimeNow extends BeforeAndAfterEach {
  this: TestSupportFixture =>

  protected val fixedDateTimeNow: DateTime = new DateTime(2017, 7, 30, 0, 0)

  override def beforeEach(): Unit = {
    super.beforeEach()

    DateTimeUtils.setCurrentMillisFixed(fixedDateTimeNow.getMillis)
  }

  override def afterEach(): Unit = {
    DateTimeUtils.setCurrentMillisOffset(0L)

    super.afterEach()
  }
}
