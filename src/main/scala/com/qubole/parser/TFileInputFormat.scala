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

import org.apache.hadoop.conf.{Configurable => TConfigurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat, CombineFileRecordReader, CombineFileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

import scala.collection.JavaConverters._

class TFileInputFormat extends CombineFileInputFormat[Text, Text] with Configurable {

  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

  override def createRecordReader(
    split: InputSplit,
    context: TaskAttemptContext): RecordReader[Text, Text] = {
    val reader =
      new ConfigurableCombineFileRecordReader(split, context, classOf[TFileRecordReader])
    reader.setConf(getConf)
    reader
  }

  /**
   * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API,
   * which is set through setMaxSplitSize
   */
  def setMinPartitions(context: JobContext, minPartitions: Int) {
    val files = listStatus(context).asScala
    val totalLen = files.map(file => if (file.isDirectory) 0L else file.getLen).sum
    val maxSplitSize = Math.ceil(totalLen * 1.0 /
      (if (minPartitions == 0) 1 else minPartitions)).toLong
    super.setMaxSplitSize(maxSplitSize)
  }
}

class ConfigurableCombineFileRecordReader[K, V](
  split: InputSplit,
  context: TaskAttemptContext,
  recordReaderClass: Class[_ <: RecordReader[K, V] with Configurable])
  extends CombineFileRecordReader[K, V](
    split.asInstanceOf[CombineFileSplit],
    context,
    recordReaderClass
  ) with Configurable {

  override def initNextRecordReader(): Boolean = {
    val r = super.initNextRecordReader()
    if (r) {
      this.curReader.asInstanceOf[Configurable].setConf(getConf)
    }
    r
  }
}

trait Configurable extends TConfigurable {
   private var conf: Configuration = _
   def setConf(c: Configuration) {
      conf = c
   }
   def getConf: Configuration = conf
}
