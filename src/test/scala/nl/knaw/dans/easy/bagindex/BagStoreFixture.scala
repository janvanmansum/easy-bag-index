package nl.knaw.dans.easy.bagindex

import java.nio.file.{ Path, Paths }

import nl.knaw.dans.easy.bagindex.components.{ BagStoreAccess, Bagit4FacadeComponent }
import org.apache.commons.io.FileUtils

trait BagStoreFixture extends TestSupportFixture with BagStoreAccess with Bagit4FacadeComponent {

  override val bagStoreBaseDir: Path = {
    val bagStoreBaseDir = testDir.resolve("bag-store")
    val origBagStore = Paths.get(getClass.getClassLoader.getResource("bag-store/").toURI)
    FileUtils.copyDirectory(origBagStore.toFile, bagStoreBaseDir.toFile)

    bagStoreBaseDir
  }
  override val bagFacade = new Bagit4Facade()
}
