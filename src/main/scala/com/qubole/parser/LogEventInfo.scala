package com.qubole.parser

class LogEventInfo {
  var sourceType: String = null
  var source: String = null
  var message: String = null
  var jobId: String = null
  var containerId: String = null
  var nodeId: String = null
  var userName: String = null
  var logLengthStr: String = null
  var logUploadTime: String = null
  var azkabanProjectName: String = null
  var azkabanFlowName: String = null
  var azkabanExecutionId: String = null
}

class LogLine {
  var timestamp: String = null
  var sourceType: String = null
  var source: String = null
  var message: String = null
  var containerId: String = null
  var nodeId: String = null
  var lineNum: String = null
}