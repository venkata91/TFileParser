package com.qubole.parser

import org.slf4j.{Logger, LoggerFactory}

import java.text.ParseException
import java.util.Date
import scala.collection.JavaConverters._

object YarnContainerLogsUtil {

  import java.util.regex.Pattern

  private val REGEX_FOR_SPLITTING_LINES = "[\r\n]+"
  private val DATE_PATTERN_FORMAT_1 = "yy/MM/dd HH:mm:ss"
  private val DATE_PATTERN_FORMAT_2 = "EEE MMM dd HH:mm:ss Z yyyy"
  private val TARGET_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss"
  private val REGEX_DATE_PATTERN_YEAR_MONTH_DAY_FORMAT_1 = "^\\d{2}/\\d{2}/\\d{2}$"
  private val REGEX_DATE_PATTERN_YEAR_MONTH_DAY_FORMAT_2 = "^\\d{4}-\\d{2}-\\d{2}$"
  private val DATE_PARTITION_FORMAT = "yyyy-MM-dd-00"
  val DELIMITER = "||||"
  private val NEWLINE_CHARACTER = "\n"
  private val COMPILED_DATE_PATTERN_FORMAT_1 = Pattern.compile(REGEX_DATE_PATTERN_YEAR_MONTH_DAY_FORMAT_1)
  private val COMPILED_DATE_PATTERN_FORMAT_2 = Pattern.compile(REGEX_DATE_PATTERN_YEAR_MONTH_DAY_FORMAT_2)
  private val REGEX_OF_NEWLINE_AND_TAB = "\n\t"
  private val COMPILED_NEWLINE_TAB_PATTERN = Pattern.compile(REGEX_OF_NEWLINE_AND_TAB)
  private val UTC_TIMEZONE = "UTC"

  import java.text.SimpleDateFormat
  import java.util
  import java.util.TimeZone

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  def parseLogs(logEventInfo: LogEventInfo, lineNumber: Long): List[LogLine] = {
    var lineNum = lineNumber
    val logLength = logEventInfo.logLengthStr.toLong
    if (logLength == 0) return null
    var logEventStructure: LogLine = null
    val logList = new util.ArrayList[LogLine]
    val text = logEventInfo.message
    val textWithTabLinesConcatenatedAsOne = COMPILED_NEWLINE_TAB_PATTERN.matcher(text).replaceAll(" ")
    val lines = textWithTabLinesConcatenatedAsOne.split(REGEX_FOR_SPLITTING_LINES)
    LOG.debug("Total lines to parse: " + lines.length)
    for (logLine <- lines) {
      val line = logLine.trim
      if (line.nonEmpty) {
        val words = line.split(" ")
        var logTimestamp = ""
        var message = ""
        try {
          if (matchesDatePatternFormat1(words(0))) {
            val dateTime = words(0) + " " + words(1)
            val originalDateFormat = new SimpleDateFormat(DATE_PATTERN_FORMAT_1)
            originalDateFormat.setTimeZone(TimeZone.getTimeZone(UTC_TIMEZONE))
            val targetDateFormat = new SimpleDateFormat(TARGET_DATE_PATTERN)
            val date = originalDateFormat.parse(dateTime)
            logTimestamp = targetDateFormat.format(date)
            message = String.join(" ", util.Arrays.copyOfRange(words, 2, words.length):_*)
          } else if (matchesDatePatternFormat2(words(0))) {
            val time = words(1).split(",")
            logTimestamp = words(0) + " " + time(0)
            message = String.join(" ", util.Arrays.copyOfRange(words, 2, words.length):_*)
          } else {
            logTimestamp = logEventInfo.logUploadTime
            message = line
          }
        } catch {
          case e: ParseException =>
            logTimestamp = logEventInfo.logUploadTime
            message = line
        }

        if (message.compareTo("") != 0) {
          logEventStructure = new LogLine
          lineNum = lineNum + 1
          logEventStructure.timestamp = logTimestamp
          logEventStructure.sourceType = logEventInfo.sourceType
          logEventStructure.source = logEventInfo.source
          logEventStructure.message = message
          logEventStructure.containerId = logEventInfo.containerId
          logEventStructure.nodeId = logEventInfo.nodeId
          logEventStructure.lineNum = lineNum.toString
          logList.add(logEventStructure)
        }
      }
    }
    logList.asScala.toList
  }

  /**
   * This method is used to verify if a given string matches a particular date pattern (yy/MM/dd)
   *
   * @param date : String containing a date
   * @return true/false if the given date matches a particular pattern.
   */
  def matchesDatePatternFormat1(date: String): Boolean = COMPILED_DATE_PATTERN_FORMAT_1.matcher(date).matches

  /**
   * This method is used to verify if a given string matches a particular date pattern (yyyy-MM-dd)
   *
   * @param date : String containing a date
   * @return true/false if the given date matches a particular pattern.
   */
  def matchesDatePatternFormat2(date: String): Boolean = COMPILED_DATE_PATTERN_FORMAT_2.matcher(date).matches

  /**
   * This method is used to convert timestamp from one format to another
   *
   * @param timeStamp : Timestamp in the following format: EEE MMM dd HH:mm:ss Z yyyy
   * @return String : Timestamp in the following format: yyyy-MM-dd HH:mm:ss
   * @throws ParseException
   */
  def getParsedTimestamp(timeStamp: String): String = {
    val originalFormat = new SimpleDateFormat(DATE_PATTERN_FORMAT_2)
    originalFormat.setTimeZone(TimeZone.getTimeZone(UTC_TIMEZONE))
    val targetFormat = new SimpleDateFormat(TARGET_DATE_PATTERN)
    targetFormat.setTimeZone(TimeZone.getTimeZone(UTC_TIMEZONE))
    val date = originalFormat.parse(timeStamp)
    val finalDate = targetFormat.format(date)
    finalDate
  }

  /**
   * This method is used to convert timestamp from one format to a datepartition format.
   * This method is used for building a datepartition string for querying.
   *
   * @param timeStamp : Timestamp in the following format: EEE MMM dd HH:mm:ss Z yyyy
   * @return String : Timestamp in the following format: yyyy-MM-dd-00
   * @throws ParseException
   */
  @throws[ParseException]
  def getDatePartitionForQuerying(timeStamp: String): String = {
    val originalFormat = new SimpleDateFormat(DATE_PATTERN_FORMAT_2)
    originalFormat.setTimeZone(TimeZone.getTimeZone(UTC_TIMEZONE))
    val targetFormat = new SimpleDateFormat(DATE_PARTITION_FORMAT)
    targetFormat.setTimeZone(TimeZone.getTimeZone(UTC_TIMEZONE))
    val date = originalFormat.parse(timeStamp)
    val finalDate = targetFormat.format(date)
    finalDate
  }

}
