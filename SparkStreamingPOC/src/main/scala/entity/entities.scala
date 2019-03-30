package entity

import java.sql.Timestamp

case class RateData(timestamp: Timestamp, value: Long)
case class Employee(id: Long, firstName: String, lastName: String, eventTime: Timestamp)
case class Department(id: Long, name: String, eventTime: Timestamp)
