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
package nl.knaw.dans.easy.bagindex.components

import java.util.UUID

import nl.knaw.dans.easy.bagindex.{ BagIndexDatabaseFixture, BagStoreFixture, Bagit5Fixture, TestSupportFixture }

import scala.util.Success

class IndexBagStoreSpec extends TestSupportFixture
  with BagStoreFixture
  with Bagit5Fixture
  with BagIndexDatabaseFixture
  with IndexBagStoreComponent
  with IndexBagStoreDatabaseComponent {

  override val database: Database = new Database {}
  override val indexDatabase: IndexBagStoreDatabase = new IndexBagStoreDatabase {}
  override val indexFull: IndexBagStore = new IndexBagStore {}

  "indexBagStore" should "walk through the bagstore and index all bag relations" in {
    val uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val uuid3 = UUID.fromString("00000000-0000-0000-0000-000000000003")

    indexFull.indexBagStore() shouldBe a[Success[_]]

    inside(database.getAllBagInfos) {
      case Success(rels) => rels.map(rel => (rel.bagId, rel.baseId, rel.doi)) should contain allOf(
        (uuid1, uuid1, doiMap(uuid1)),
        (uuid2, uuid1, doiMap(uuid2)),
        (uuid3, uuid1, doiMap(uuid3))
      )
    }
  }
}
