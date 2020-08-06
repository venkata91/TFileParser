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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.file.tfile.TFile
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import java.io.BufferedReader
import java.io.EOFException
import java.io.IOException
import java.io.InputStreamReader

/**
  * Simple record reader which reads the TFile and emits it as key, value pair.
  * If value has multiple lines, read one line at a time.
  */

class TFileRecordReader extends RecordReader[Text, Text] {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[TFileRecordReader])

  private var start: Long = 0L
  private var end: Long = 0L
  protected var splitPath: Path = null
  private var fin: FSDataInputStream = null
  protected var reader: TFile.Reader = null
  protected var scanner: TFile.Reader.Scanner = null
  private var key: Text = new Text
  private var value: Text = new Text
  private val keyBytesWritable: BytesWritable = new BytesWritable
  private var currentValueReader: BufferedReader = null

  @throws[IOException]
  @throws[InterruptedException]
  def initialize(split: InputSplit, context: TaskAttemptContext) {
    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
    LOG.info("Initializing TFileRecordReader : " + fileSplit.getPath.toString)
    start = fileSplit.getStart
    end = start + fileSplit.getLength
    val fs: FileSystem = fileSplit.getPath.getFileSystem(context.getConfiguration)
    splitPath = fileSplit.getPath
    fin = fs.open(splitPath)
    reader = new TFile.Reader(fin, fs.getFileStatus(splitPath).getLen, context.getConfiguration)
    scanner = reader.createScannerByByteRange(start, fileSplit.getLength)
  }

  @throws[IOException]
  private def populateKV(entry: TFile.Reader.Scanner#Entry) {
    LOG.debug("Populating key values of the TFile")
    entry.getKey(keyBytesWritable)
    //splitpath contains the machine name. Create the key as splitPath + realKey
    val keyStr: String = new StringBuilder().append(splitPath.getName).append(":").append(new String(keyBytesWritable.getBytes)).toString

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
  def getProgress: Float = ((fin.getPos - start) * 1.0f) / ((end - start) * 1.0f)

  @throws[IOException]
  def close() {
    IOUtils.closeQuietly(scanner)
    IOUtils.closeQuietly(reader)
    IOUtils.closeQuietly(fin)
  }
}
