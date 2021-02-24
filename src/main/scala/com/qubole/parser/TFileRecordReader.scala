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

import org.apache.commons.collections4.iterators.PeekingIterator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import java.io.IOException

class TFileRecordReader extends RecordReader[Text, Text] {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[TFileRecordReader])
  private[this] val key: Text = new Text()
  private[this] val value: Text = new Text

  private var iterator: PeekingIterator[LogLine] = null

  @throws[IOException]
  @throws[InterruptedException]
  def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit = inputSplit.asInstanceOf[FileSplit]
    val sourceTypesToRead = context.getConfiguration.get("hadoop.tfile.source.types.read", "")
          .split(",")
          .toSet
    LOG.info(s"Source types to be read $sourceTypesToRead")
    iterator = new YarnContainerLogReader(fileSplit.getPath, sourceTypesToRead).read()
  }

  @throws[IOException]
  @throws[InterruptedException]
  def nextKeyValue: Boolean = {
    if (iterator.hasNext) {
      val record = iterator.next()
      if (record != null) {
        key.set(record.source + "," + record.nodeId + "," + record.containerId + "," + record.sourceType)
        value.set(record.message)
        true
      } else {
        false
      }
    } else {
      false
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
  override def getProgress: Float = 1.0f

  override def close(): Unit = {
    iterator = null
  }
}
