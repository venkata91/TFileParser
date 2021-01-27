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
package org.apache.spark

import com.qubole.parser.TFileInputFormat
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl
import org.apache.spark.rdd.{NewHadoopPartition, NewHadoopRDD, RDD}

class TFileRDD(sc: SparkContext,
    inputFormatClass: Class[_ <: TFileInputFormat],
    keyClass: Class[Text],
    valueClass: Class[Text],
    conf: Configuration,
    minPartitions: Int)
  extends NewHadoopRDD[Text, Text](sc, inputFormatClass, keyClass, valueClass, conf) {

  override def getPartitions: Array[Partition] = {
    val conf = getConf
    // setMinPartitions below will call FileInputFormat.listStatus(), which can be quite slow when
    // traversing a large number of directories and files. Parallelize it.
    conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    val inputFormat = inputFormatClass.newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }

    val jobContext = new JobContextImpl(conf, jobId)
    inputFormat.setMinPartitions(jobContext, minPartitions)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}

object TFileRDD {
  def apply(sc: SparkContext,
      path: String,
      minPartitions: Int = 2): RDD[(String, String)] = {
    val job = Job.getInstance(sc.hadoopConfiguration)
    FileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new TFileRDD(
      sc,
      classOf[TFileInputFormat],
      classOf[Text],
      classOf[Text],
      updateConf,
      minPartitions).map(record => (record._1.toString, record._2.toString)).setName(path)
  }
}