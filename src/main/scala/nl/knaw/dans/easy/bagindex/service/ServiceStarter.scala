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

import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.daemon.{ Daemon, DaemonContext }

class ServiceStarter extends Daemon with DebugEnhancedLogging {
  var bagIndexService: BagIndexService = _

  def init(context: DaemonContext): Unit = {
    logger.info("Initializing service...")
    bagIndexService = BagIndexService()
    logger.info("Service initialized.")
  }

  def start(): Unit = {
    logger.info("Starting service...")
    bagIndexService.start()
      .map(_ => logger.info("Service started."))
      .recover { case t => logger.error("Service failed to start", t) }
  }

  def stop(): Unit = {
    logger.info("Stopping service...")
    bagIndexService.stop()
  }

  def destroy(): Unit = {
    bagIndexService.destroy
    logger.info("Service stopped.")
  }
}
