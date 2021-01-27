/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  * <p/>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p/>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.qubole.parser

import org.apache.commons.io.IOUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.file.tfile.TFile
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit

import java.io.{BufferedReader, EOFException, IOException, InputStreamReader}

/**
  * Simple record reader which reads the TFile and emits it as key, value pair.
  * If value has multiple lines, read one line at a time.
  */

class TFileRecordReader(split: CombineFileSplit, context: TaskAttemptContext, index: Integer)
  extends RecordReader[Text, Text] with Configurable {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[TFileRecordReader])

  private[this] val path = split.getPath(index)
  // True means the current file has been processed, then skip it.
  private[this] var processed = false

  private[this] var key: Text = new Text(path.toString)
  private[this] var value: Text = new Text

  val fs: FileSystem = path.getFileSystem(context.getConfiguration)
  var reader: TFile.Reader = null
  var scanner: TFile.Reader.Scanner = null

  private val keyBytesWritable: BytesWritable = new BytesWritable
  private var fin: FSDataInputStream = null
  private var currentValueReader: BufferedReader = null

  @throws[IOException]
  @throws[InterruptedException]
  def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    fin = fs.open(path)
    reader = new TFile.Reader(fin, fs.getFileStatus(path).getLen, context.getConfiguration)
    scanner = reader.createScannerByByteRange(0, split.getLength(index))
  }

  @throws[IOException]
  private def populateKV(entry: TFile.Reader.Scanner#Entry) {
    LOG.debug("Populating key values of the TFile")
    entry.getKey(keyBytesWritable)
    //splitpath contains the machine name. Create the key as splitPath + realKey
    val keyStr: String = new StringBuilder().append(path.toString).append(":").append(new String(keyBytesWritable.getBytes)).toString

    /**
     * In certain cases, values can be huge (files > 2 GB). Stream is
     * better to handle such scenarios.
     */
    currentValueReader = new BufferedReader(new InputStreamReader(entry.getValueStream))
    key.set(keyStr)
    val line: String = currentValueReader.readLine
    value.set(if (line == null) ""
    else line)
  }

  @throws[IOException]
  @throws[InterruptedException]
  def nextKeyValue: Boolean = {
    LOG.debug("next key value pair")
    if (currentValueReader != null) {
      //Still at the old entry reading line by line
      val line: String = currentValueReader.readLine
      if (line != null) {
        value.set(line)
        return true
      }
      else {
        //Read through all lines in the large value stream. Move to next KV.
        scanner.advance
      }
    }
    try {
      populateKV(scanner.entry)
      true
    } catch {
      case _: EOFException => {
        key = null
        value = null
        false
      }
    }
  }

  @throws[IOException]
  @throws[InterruptedException]
  def getCurrentKey: Text = key

  @throws[IOException]
  @throws[InterruptedException]
  def getCurrentValue: Text = value

  @throws[IOException]
  @throws[InterruptedException]
  override def getProgress: Float = if (processed) 1.0f else 0.0f

  @throws[IOException]
  def close() {
    IOUtils.closeQuietly(scanner)
    IOUtils.closeQuietly(reader)
    IOUtils.closeQuietly(fin)
  }
}
