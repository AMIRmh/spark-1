
package org.apache.spark.sql.execution.streaming.state

import java.io._
import java.nio.file.{Files, Paths}

import org.apache.spark.internal.Logging


object RocksDBStateStoreBuffer extends Logging {
  var in: Option[FileInputStream] = None
  var out: Option[FileOutputStream] = None
  var position = 0
  var filePath = "/tmp/sparkBuffer/cache"
  private var getFileIndex = 0
  private var writeFileIndex = 0
  private var writeCounter = 0
  private val writeCacheSize = 15000
  private val writeCache = new Array[Array[Byte]](writeCacheSize)
  private var getSnapPositionArray = new Array[Int](0)

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
    val file = openFile(filePath + writeFileIndex)
    //    in = Some(new FileInputStream(file))
    out = Some(new FileOutputStream(file))
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    if (out.isEmpty) {
      init()
    }
    ReadWriteLocker.synchronized {
      if (writeCounter % writeCache.length == 0 && writeCounter != 0) {
        val cache = writeCacheToArray()
        getSnapPositionArray = getSnapPositionArray :+ cache.length
        out.get.write(cache)
        writeFileIndex += 1
        init()
//        out.get.write(new KeyValueStruct(key, value).ToArray())
      }
      writeCache(writeCounter % writeCacheSize) = new KeyValueStruct(key, value).ToArray()
      writeCounter += 1
    }
  }

  private def writeCacheToArray(): Array[Byte] = {
    var sumSize = 0
    for (kv <- writeCache) {
      sumSize += kv.length
    }
    val result = new Array[Byte](sumSize)
    var pos = 0
    for (kv <- writeCache) {
      Array.copy(kv, 0, result, pos, kv.length)
      pos += kv.length
    }
    result
  }

  def get(): Array[KeyValueStruct] = {
    try {
      var bytes: Array[Byte] = new Array[Byte](0)
      bytes = Files.readAllBytes(Paths.get(filePath + getFileIndex))
      KeyValueStruct.ParseBytes(bytes, writeCounter)
//      ReadWriteLocker.synchronized {
//        out.get.flush()
//        out.get.getChannel.position(0)
//        bytes = Files.readAllBytes(Paths.get(filePath))
//        init()
//        KeyValueStruct.ParseBytes(bytes, writeCounter)
//      }
    } finally {
      writeCounter = 0
      Files.delete(Paths.get(filePath + getFileIndex))
      getFileIndex += 1

    }
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
    val keyZero = new Array[Byte](4 - keyLenOrig.length)
    //    val keyLen = new Array[Byte](4 - keyLenOrig.length) ++ keyLenOrig
    val keyLen = new Array[Byte](keyLenOrig.length + keyZero.length)
    Array.copy(keyZero, 0, keyLen, 0, keyZero.length)
    Array.copy(keyLenOrig, 0, keyLen, keyZero.length, keyLenOrig.length)


    val valLenOrig = BigInt(value.length).toByteArray
    val valueZero = new Array[Byte](4 - valLenOrig.length)
//    val valLen = new Array[Byte](4 - valLenOrig.length) ++ valLenOrig
    val valLen = new Array[Byte](valLenOrig.length + valueZero.length)
    Array.copy(valueZero, 0, valLen, 0, valueZero.length)
    Array.copy(valLenOrig, 0, valLen, valueZero.length, valLenOrig.length)


    val result = new Array[Byte](keyLen.length + key.length + valLen.length + value.length)
    var pos = 0
    Array.copy(keyLen, 0, result, pos, keyLen.length)
    pos += keyLen.length
    Array.copy(key, 0, result, pos, key.length)
    pos += key.length
    Array.copy(valLen, 0, result, pos, valLen.length)
    pos += valLen.length
    Array.copy(value, 0, result, pos, value.length)
    result
    //    keyLen ++ key ++ valLen ++ value
  }
}

object KeyValueStruct {
  def ParseBytes(bytes: Array[Byte], counter: Int): Array[KeyValueStruct] = {
    val list: Array[KeyValueStruct] = new Array[KeyValueStruct](counter)
    val len = bytes.length
    var pos = 0
    var i = 0;
    while (pos < len) {
      var size = BigInt(bytes.slice(pos, pos + 4)).intValue()
      pos += 4
      val key = bytes.slice(pos, pos + size)
      pos += size
      size = BigInt(bytes.slice(pos, pos + 4)).intValue()
      pos += 4
      val value = bytes.slice(pos, pos + size)
      pos += size
      list(i) = new KeyValueStruct(key, value)
      i += 1
      //      list = list :+ new KeyValueStruct(key, value)
    }

    list
  }
}
