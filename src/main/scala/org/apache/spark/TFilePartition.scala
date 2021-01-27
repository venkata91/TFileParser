package org.apache.spark

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.rdd.NewHadoopPartition

class TFilePartition(
    rddId: Int,
    override val index: Int,
    rawSplit: InputSplit with Writable)
  extends NewHadoopPartition(rddId = rddId, index = index, rawSplit = rawSplit)