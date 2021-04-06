package com.databricks.labs.overwatch.utils

import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.nio.file.Files

class HelpersTest extends AnyFunSpec {

  describe("HelpersTest") {
    it("should parListFiles") {
      val path = Files.createTempDirectory("qqqwqw")
      val topDir = path.toAbsolutePath.toFile
      val file1 = File.createTempFile("asqw", "dqwq", topDir)
      val file2 = File.createTempFile("asqw22", "dqwq", topDir)

      val expected = Seq(file1.getCanonicalPath, file2.getCanonicalPath).map(x => s"file:$x").sorted
      try {
        assertResult(expected)(Helpers.parListFiles(topDir.getCanonicalPath).sorted)
      } finally {
        Files.deleteIfExists(file1.toPath)
        Files.deleteIfExists(file2.toPath)
        Files.deleteIfExists(path)
      }
    }

    it("should pathExists") {
      val file = File.createTempFile("aserwe", "qd2q")
      file.deleteOnExit()
      assertResult(true)(Helpers.pathExists(file.getCanonicalPath))
    }

    it("should directoryExists with dir") {
      val path = Files.createTempDirectory("qqqwqw")
      try {
        assertResult(true)(Helpers.directoryExists(path.toAbsolutePath.toString))
      } finally {
        Files.deleteIfExists(path)
      }
    }

    it("should directoryExists with file") {
      val file = File.createTempFile("aserwe", "qd2q")
      file.deleteOnExit()
      assertResult(false)(Helpers.directoryExists(file.getCanonicalPath))
    }

    it("should rmSer") {
      val file1 = File.createTempFile("dasd", "dqwq")
      try {
        assertResult(true)(Files.exists(file1.toPath))
        Helpers.rmSer(file1.getCanonicalPath)
        assertResult(false)(Files.exists(file1.toPath))
      } finally {
        Files.deleteIfExists(file1.toPath)
      }
    }
  }
}
