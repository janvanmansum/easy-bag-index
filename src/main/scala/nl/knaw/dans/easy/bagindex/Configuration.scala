package nl.knaw.dans.easy.bagindex

import java.nio.file.{ Files, Paths }

import org.apache.commons.configuration.PropertiesConfiguration
import resource.managed

import scala.io.Source

trait Configuration {
  def version: String
  def properties: PropertiesConfiguration
}

trait DefaultConfiguration extends Configuration {
  private val home = Paths.get(System.getProperty("app.home"))

  override val version: String = managed(Source.fromFile(home.resolve("bin/version").toFile)).acquireAndGet(_.mkString)

  private val cfgPath = Seq(
    Paths.get(s"/etc/opt/dans.knaw.nl/easy-bag-index/"),
    home.resolve("cfg"))
    .find(Files.exists(_))
    .getOrElse { throw new IllegalStateException("No configuration directory found")}
  override val properties = new PropertiesConfiguration(cfgPath.resolve("application.properties").toFile)
}
