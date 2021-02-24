package com.qubole.parser

import org.apache.commons.collections4.iterators.PeekingIterator
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat
import org.slf4j.{Logger, LoggerFactory}

import java.io.{DataInputStream, EOFException, IOException}
import java.util.ArrayList
import scala.collection.JavaConverters._

class YarnContainerLogReader(
  logFilePath: Path,
  sourceTypesToRead: Set[String] = Set.empty[String]){
  private val LOG: Logger = LoggerFactory.getLogger(classOf[YarnContainerLogReader])

  private val TMP_FILE_SUFFIX = ".tmp"
  private val REGEX_FOR_SPLITTING_LINES = "[\r\n]+"
  private val DELIMITER = "\\|\\|\\|\\|"

  def read(): PeekingIterator[LogLine] = {
    val conf = new YarnConfiguration()
    val pathString = logFilePath.toString
    val fs = logFilePath.getFileSystem(conf)

    if (!pathString.endsWith(TMP_FILE_SUFFIX) && fs.getFileStatus(logFilePath).getLen > 0L) {
      val reader = new AggregatedLogFormat.LogReader(conf, logFilePath)
      val containerId = new AggregatedLogFormat.LogKey()
      val valueStream = reader.next(containerId)

      val logEventInfo = new LogEventInfo
      logEventInfo.source = logFilePath.toString
      logEventInfo.nodeId = logFilePath.getName
      logEventInfo.containerId = containerId.toString

      val fileType = valueStream.readUTF()
      val fileLength = valueStream.readUTF()

      logEventInfo.sourceType = fileType
      logEventInfo.logLengthStr = fileLength
      new PeekingIterator[LogLine](new YarnContainerLogIterator(reader, valueStream, logFilePath, logEventInfo, sourceTypesToRead))
    } else {
      new PeekingIterator[LogLine](Array.empty[LogLine].iterator.asJava)
    }
  }
}

class YarnContainerLogIterator(
  reader: AggregatedLogFormat.LogReader,
  var valueStream: DataInputStream,
  logFilePath: Path,
  var logEventInfo: LogEventInfo,
  val sourceTypesToRead: Set[String]) extends java.util.Iterator[LogLine] {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[YarnContainerLogIterator])

  val DELIMITER = "\\|\\|\\|\\|"

  val applicationLogList = new ArrayList[LogLine] //List to store the logs read.
  var logsFetched: String = null //A chunk of logs fetched till the last occurrence of newline character.
  var remainingLogs = "" //Remaining logs left from the last occurrence of newline character from the logs fetched.
  var fileLength = logEventInfo.logLengthStr.toLong //Length of the log file for a specific container for a specific log type(e.g syslog, stderr).
  var lineNumber = 1L //Line number of the log line for a specific container for a specific log type.
  var curRead = 0L //Total length of the logs that have been read.
  var toRead = 0 //Maximum number of bytes to be read in the buffer.
  var len = 0 //The total number of bytes read into the buffer.
  var buffer = new Array[Byte](65535) //The buffer into which the logs are read.

  updateMaxNumBytesToRead

  override def hasNext(): Boolean = {
    if (applicationLogList.isEmpty) {
      try {
        if (len == -1 || curRead >= fileLength) {
          //This indicates that logs of a specific log type(e.g syslog, stderr) for a specific container are either empty or finished.
          //Hence we move to read logs of next log type for the same container id.
          if (valueStream != null) {
            //This indicates that we will move to read logs of next log type for the same container id.
            resetCountersForNewLogType
            while (!sourceTypesToRead.contains(logEventInfo.sourceType) && toRead > 0) {
              skipToNextSourceType
            }
            readSomeBytesOfLogs
          } else {
            //This indicates that all the logs of the node file have been read. Hence closing the reader.
            LOG.info("Closing the reader.")
            reader.close()
          }
        } else {
          //This indicates that we continue to read logs of the same log type for the same container id.
          readSomeBytesOfLogs
        }
      } catch {
        case _: EOFException =>
          //This indicates that we have finished reading logs of one container. Hence we move to read logs of next container.
          val containerId = new AggregatedLogFormat.LogKey()
          valueStream = reader.next(containerId)
          if (valueStream != null) {
            //Next Container exists. Read logs of the next container.
            this.logEventInfo = new LogEventInfo
            logEventInfo.source = logFilePath.toString
            logEventInfo.containerId = containerId.toString
            logEventInfo.nodeId = logFilePath.getName
            resetCountersForNewLogType
            readSomeBytesOfLogs
          } else {
            //This indicates that all the logs of the node file have been read. Hence closing the reader.
            LOG.info("Closing the reader.")
            reader.close()
          }
      }
    }
    !applicationLogList.isEmpty
  }

  override def next(): LogLine = {
    hasNext()
    var logLine: LogLine = null
    try {
      logLine = applicationLogList.get(0)
      applicationLogList.remove(0)
      logLine
    } catch {
      case e: NoSuchElementException =>
        LOG.error("Unable to fetch next line: ", e)
        logLine
    }
  }

  @throws[IOException]
  private def readSomeBytesOfLogs(): Unit = {
    len = valueStream.read(buffer, 0, toRead)
    if (len != -1) {
      val logsRead = new String(buffer, 0, len)
      val index = logsRead.lastIndexOf('\n')
      logsFetched = logsRead.substring(0, index + 1)
      val logMessage = new StringBuilder
      logMessage.append(remainingLogs).append(logsFetched)

      val beginIndex = index + 1
      val subLen = logsRead.length - beginIndex

      if (subLen >= 0) remainingLogs = logsRead.substring(index + 1)
      else remainingLogs = ""

      logEventInfo.message = logMessage.toString
      val containerLogsForLogType: List[LogLine] = YarnContainerLogsUtil.parseLogs(logEventInfo, lineNumber)

      if (containerLogsForLogType != null && !containerLogsForLogType.isEmpty) {
        lineNumber = getLineNum(containerLogsForLogType)
        applicationLogList.addAll(containerLogsForLogType.asJava)
      }
      curRead += len
      updateMaxNumBytesToRead
    }
  }

  @throws[IOException]
  private def resetCountersForNewLogType: Unit = {
    val fileType = valueStream.readUTF()
    val fileLengthStr = valueStream.readUTF()

    logEventInfo.sourceType = fileType
    logEventInfo.logLengthStr = fileLengthStr
    fileLength = fileLengthStr.toLong
    remainingLogs = ""
    curRead = 0
    lineNumber = 1
    updateMaxNumBytesToRead
  }

  private def skipToNextSourceType: Unit = {
    LOG.info(s"Skipping source type ${logEventInfo.sourceType} from the file ${logEventInfo.source}")
    try {
      while (toRead > 0) {
        len = valueStream.read(buffer, 0, toRead)
        if (len != -1) {
          curRead += len
          updateMaxNumBytesToRead
        }
      }
      resetCountersForNewLogType
    } catch {
      case _: EOFException =>
        LOG.warn(s"Reached end of file for ${logEventInfo.source} with type ${logEventInfo.sourceType}")
    }
  }

  private def updateMaxNumBytesToRead: Unit = {
    val pendingRead = fileLength - curRead
    toRead = if (pendingRead > buffer.length) buffer.length else pendingRead.toInt
  }

  private def getLineNum(records: List[LogLine]): Long = {
    val size = records.size
    val temp = records(size - 1)
    temp.lineNum.toLong
  }
}
