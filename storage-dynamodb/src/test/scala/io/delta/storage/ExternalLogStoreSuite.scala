/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage

import java.io.File
import java.net.URI

import org.apache.hadoop.fs._
import org.apache.spark.sql.delta.FakeFileSystem
import org.apache.spark.sql.delta.util.FileNames

class ExternalLogStoreSuite extends org.apache.spark.sql.delta.PublicLogStoreSuite {
  override protected val publicLogStoreClassName: String =
    classOf[MemoryLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme \"fake\"",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true"
  )

  def getDeltaVersionPath(logDir: File, version: Int): Path = {
    FileNames.deltaFile(new Path(logDir.toURI), version)
  }

  def getFailingDeltaVersionPath(logDir: File, version: Int): Path = {
    FileNames.deltaFile(new Path(s"failing:${logDir.getCanonicalPath}"), 0)
  }

  test("single write") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val path = getDeltaVersionPath(tempLogDir, 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      val entry = MemoryLogStore.get(path);
      assert(entry != null)
      assert(entry.complete);
    }
  }

  test("double write") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val path = getDeltaVersionPath(tempLogDir, 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
      assertThrows[java.nio.file.FileSystemException] {
        store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      }
    }
  }

  test("overwrite") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val path = getDeltaVersionPath(tempLogDir, 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
      store.write(path, Iterator("foo", "bar"), overwrite = true, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
    }
  }

  test("write N+1 recovers version N") {
    withSQLConf(
      "fs.failing.impl" -> classOf[FailingFileSystem].getName,
      "fs.failing.impl.disable.cache" -> "true"
    ) {
      withTempLogDir { tempLogDir =>
        val store = createLogStore(spark)

        val delta0_normal = getDeltaVersionPath(tempLogDir, 0)
        val delta0_fail = getFailingDeltaVersionPath(tempLogDir, 0)
        val delta1 = getDeltaVersionPath(tempLogDir, 1)

        // Case 1: write N+1 fails
        // - entry for delta0 doesn't exist in external store or in file system
        val e = intercept[java.nio.file.FileSystemException] {
          store.write(delta1, Iterator("one"), overwrite = false, sessionHadoopConf)
        }
        assert(e.getMessage == s"previous commit $delta0_normal doesn't exist")

        // Case 2: write N+1 succeeds
        // - entry for delta0 does exist in external store but not in file system
        FailingFileSystem.failOnSuffix = Some(delta0_fail.getName)
        store.write(delta0_fail, Iterator("zero"), overwrite = false, sessionHadoopConf)
        assert(!delta0_fail.getFileSystem(sessionHadoopConf).exists(delta0_fail))
        assert(!MemoryLogStore.get(delta0_fail).complete)
        assert(MemoryLogStore.get(delta0_fail) == MemoryLogStore.get(delta0_normal))

        store.write(delta1, Iterator("one"), overwrite = false, sessionHadoopConf)
        assert(delta0_fail.getFileSystem(sessionHadoopConf).exists(delta0_fail))
        assert(MemoryLogStore.get(delta0_fail).complete)
        assert(MemoryLogStore.get(delta1).complete)
      }
    }
  }

  test("listFrom performs recovery") {
    withSQLConf(
      "fs.failing.impl" -> classOf[FailingFileSystem].getName,
      "fs.failing.impl.disable.cache" -> "true"
    ) {
      withTempLogDir { tempLogDir =>
        val store = createLogStore(spark)
        val delta0_normal = getDeltaVersionPath(tempLogDir, 0)
        val delta0_fail = getFailingDeltaVersionPath(tempLogDir, 0)

        // fail to write to FileSystem when we try to commit 0000.json
        FailingFileSystem.failOnSuffix = Some(delta0_fail.getName)

        // try and commit 0000.json
        store.write(delta0_fail, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)

        // check that entry was written to external store and that it doesn't exist in FileSystem
        val entry = MemoryLogStore.get(delta0_fail)
        assert(entry != null)
        assert(!entry.complete)
        assert(entry.tempPath.nonEmpty)
        assert(!delta0_fail.getFileSystem(sessionHadoopConf).exists(delta0_fail))

        // Now perform a `listFrom` read, which should fix the transaction log
        val contents = store.read(entry.absoluteTempPath()).toList
        FailingFileSystem.failOnSuffix = None
        store.listFrom(delta0_normal, sessionHadoopConf)
        val entry2 = MemoryLogStore.get(delta0_normal)
        assert(entry2 != null)
        assert(entry2.complete)
        assert(store.read(entry2.absoluteFilePath(), sessionHadoopConf).toList == contents)
      }
    }
  }

  test("write to new Delta table but a DynamoDB entry for it already exists") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)

      // write 0000.json
      val path = getDeltaVersionPath(tempLogDir, 0)
      store.write(path, Iterator("foo"), overwrite = false, sessionHadoopConf)

      // delete 0000.json from FileSystem
      val fs = path.getFileSystem(sessionHadoopConf)
      fs.delete(path, false)

      // try and write a new 0000.json, while the external store entry still exists
      val e = intercept[java.nio.file.FileSystemException] {
        store.write(path, Iterator("bar"), overwrite = false, sessionHadoopConf)
      }.getMessage

      val tablePath = path.getParent.getParent
      assert(e == s"Old entries for table ${tablePath} still exist in the external store")
    }
  }

  test("listFrom exceptions") {
    val store = createLogStore(spark)
    assertThrows[java.io.FileNotFoundException] {
      store.listFrom("/non-existing-path/with-parent")
    }
  }

  protected def shouldUseRenameToWriteCheckpoint: Boolean = false
}

/**
 * This utility enables failure simulation on file system.
 * Providing a matching suffix results in an exception being
 * thrown that allows to test file system failure scenarios.
 */
class FailingFileSystem extends RawLocalFileSystem {
  override def getScheme: String = FailingFileSystem.scheme

  override def getUri: URI = FailingFileSystem.uri

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream = {

    FailingFileSystem.failOnSuffix match {
      case Some(suffix) =>
        if (path.toString.endsWith(suffix)) {
          throw new java.nio.file.FileSystemException("fail")
        }
      case None => ;
    }
    super.create(path, overwrite)
  }
}

object FailingFileSystem {
  private val scheme = "failing"
  private val uri: URI = URI.create(s"$scheme:///")

  var failOnSuffix: Option[String] = None
}
