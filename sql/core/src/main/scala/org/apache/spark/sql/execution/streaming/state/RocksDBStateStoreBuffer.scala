
package org.apache.spark.sql.execution.streaming.state

import java.io._
import java.nio.file.{Files, Paths}

import org.apache.spark.internal.Logging



object RocksDBStateStoreBuffer extends Logging {
  var in: Option[FileInputStream] = None
  var out: Option[FileOutputStream] = None
  var position = 0
  var filePath = "/tmp/cache"

  private object ReadWriteLocker

  private def openFile(filePath: String): File = {
    val path = Paths.get(filePath)
    val file = path.toFile

    if (file.exists()) file.delete()

//    if (!file.exists()) file.createNewFile()
    file.createNewFile()

    file
  }

  def init(): Unit = {
    var file = openFile(filePath)
//    in = Some(new FileInputStream(file))
    out = Some(new FileOutputStream(file))
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    ReadWriteLocker.synchronized {
      out.get.write(new KeyValueStruct(key, value).ToArray())
    }
  }

  def get(): Array[KeyValueStruct] = {
    var bytes: Array[Byte] = new Array[Byte](0)
    ReadWriteLocker.synchronized {
      out.get.flush()
      //      in.get.getChannel.position(0)
      out.get.getChannel.position(0)
      bytes = Files.readAllBytes(Paths.get(filePath))
      //      in.get.read()
      init()
    }
    KeyValueStruct.ParseBytes(bytes)
  }
}


class KeyValueStruct(key: Array[Byte], value: Array[Byte]) {
  def getKey: Array[Byte] = {
    key
  }

  def getValue: Array[Byte] = {
    value
  }
  def ToArray(): Array[Byte] = {

    val keyLenOrig = BigInt(key.length).toByteArray
    val keyLen = new Array[Byte](4 - keyLenOrig.length) ++ keyLenOrig


    val valLenOrig = BigInt(value.length).toByteArray
    val valLen = new Array[Byte](4 - valLenOrig.length) ++ valLenOrig
    keyLen ++ key ++ valLen ++ value
  }
}

object KeyValueStruct {
  def ParseBytes(bytes: Array[Byte]): Array[KeyValueStruct] = {
    var list: Array[KeyValueStruct] = new Array[KeyValueStruct](0)
    val len = bytes.length
    var pos = 0
    while (pos < len) {
      var size = BigInt(bytes.slice(pos, pos + 4)).intValue()
      pos += 4
      val key = bytes.slice(pos, pos + size)
      pos += size
      size = BigInt(bytes.slice(pos, pos + 4)).intValue()
      pos += 4
      val value = bytes.slice(pos, pos + size)
      pos += size
      list = list :+ new KeyValueStruct(key, value)
    }
    list
  }
}
