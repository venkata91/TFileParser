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
package com.qubole.rdd

import com.qubole.parser.TFileInputFormat
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobContext}
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.hadoop.mapreduce.{Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.spark.input.WholeTextFileInputFormat
import org.apache.spark.{Partition, SerializableWritable, SparkContext}
import org.apache.spark.rdd.{NewHadoopPartition, NewHadoopRDD, RDD, WholeTextFileRDD}

class TFileRDD(
                sc: SparkContext,
                inputFormatClass: Class[TFileInputFormat],
                keyClass: Class[Text],
                valueClass: Class[Text],
                conf: Configuration,
                minPartitions: Int) extends NewHadoopRDD[Text, Text](sc, inputFormatClass, keyClass, valueClass, conf) {
  override def getPartitions: Array[Partition] = {
    val inputFormat = inputFormatClass.newInstance
    val conf = getConf
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }

    val jobContext = new JobContextImpl(conf, jobId)
    val rawSplits = inputFormat.getSplits(jobContext)
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new TFilePartition(id, i, rawSplits.get(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }

  class TFilePartition(
                        rddId: Int,
                        val index: Int,
                        rawSplit: InputSplit with Writable)
    extends Partition {

    val serializableHadoopSplit = new SerializableWritable(rawSplit)

    override def hashCode(): Int = 31 * (31 + rddId) + index

    override def equals(other: Any): Boolean = super.equals(other)
  }
}

object TFileRDD {
  def apply(
             sc: SparkContext,
             path: String,
             minPartitions: Int = 2): RDD[(Text, Text)] = {
    sc.newAPIHadoopFile(path, classOf[TFileInputFormat], classOf[Text], classOf[Text])
  }
}