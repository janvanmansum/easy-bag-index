package nl.knaw.dans.easy.bagindex

import org.apache.commons.configuration.PropertiesConfiguration

trait ConfigurationSupportFixture {
  this: TestSupportFixture with BagIndexDatabaseFixture with BagStoreFixture =>

  trait TestConfiguration extends Configuration {
    override def version: String = "test-version"

    override def properties: PropertiesConfiguration = {
      new PropertiesConfiguration() {
        this.addProperty("bag-index.database.driver-class", dbDriverClassName)
        this.addProperty("bag-index.database.url", dbUrl)
        this.addProperty("bag-index.bag-store.base-dirs", initBagStores.toString)
      }
    }
  }
}
