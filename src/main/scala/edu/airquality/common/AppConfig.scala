package edu.airquality.common

case class AppConfig(dataFileName: String = "/var/tmp/data/data.csv", corruptedRecordsDir: String = "/var/tmp/corrupted") {}
