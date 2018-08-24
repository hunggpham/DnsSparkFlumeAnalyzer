package com.madison.sparkstreaming.sparkstreamingflume

import java.io.{InputStreamReader, BufferedReader}


import com.madison.sparkstreaming.sparkstreamingflume.{HbaseTabPutFormat, DnsLogParser, DnsLogRecord}
import org.apache.hadoop.hbase.client._

import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.log4j.Logger
import com.madison.sparkstreaming.IntParam

import java.util.regex.Pattern
import java.util.regex.Matcher

import com.google.common.io.{ByteStreams, Files}


case class DnsLogRecord (
  date_Stamp: String,
  time_Stamp: String,
  client_Ip: String,
  domain_Name: String,
  dns_Server_Ip: String
)

case class HbaseTabPutFormat(empIdDomainName: Array[Byte], colFamily: Array[(Array[Byte], Array[Byte], Array[Byte])])


class DnsLogParser extends Serializable {

  private val dateStamp = "([^\\s]*)"
  private val timeStamp = "([^\\s]*)"
  private val clientIp = "([^\\s]*)"
  private val domain = "([^\\s]*)"
  private val dnsServerIp = "([^\\s]*)"
  private val regexStr = s"<[^\\s]*>[^\\s]*[\\s]*[^\\s]*\\s[^\\s]*\\s[^\\s]*\\s[^\\s]*\\s$dateStamp\\s$timeStamp\\s[^\\s]*\\s[^\\s]*\\s$clientIp#[^\\s]*\\s\\([^\\s]*\\):\\s[^\\s]*\\s$domain\\s[^\\s]*\\s[^\\s]*\\s\\+\\s\\($dnsServerIp\\)"
  private val pattern = Pattern.compile(regexStr)


  def patternMatch(record: String): Option[DnsLogRecord] = {
    val matcher = pattern.matcher(record)
    if(matcher.find) {
      Some(buildDnsLogRecord(matcher))
    } else {
      None
    }
  }

  def nullPatternMatch(record: String): DnsLogRecord = {
    val matcher = pattern.matcher(record)
    if(matcher.find) {
      buildDnsLogRecord(matcher)
    } else {
      DnsLogParser.nullDnsLogRecord
    }
  }

  private def buildDnsLogRecord(matcher: Matcher) = {
    DnsLogRecord(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5))
  }
}

object DnsLogParser {

  val nullDnsLogRecord = DnsLogRecord("","","","","")

}

object DnsSparkFlumeAnalyzer {


  def convertDnsBatchRecsToDFs(Hc: HiveContext, dnsBatchRec: RDD[DnsLogRecord]): DataFrame = {
    val schemaString = "dateStamp timeStamp clientIp domain dnsServerIp"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = dnsBatchRec.map(dnsRec => Row(dnsRec.date_Stamp, dnsRec.time_Stamp, dnsRec.client_Ip, dnsRec.domain_Name, dnsRec.dns_Server_Ip))
    val dnsDataFrame = Hc.createDataFrame(rowRDD, schema)
    dnsDataFrame
  }


  def ConvertToHbasePut (  df: DataFrame ): RDD[HbaseTabPutFormat] = {
    df.map(k => {
      // rowId which is a composite of emp_num and domain
      val hbaseRowId = ("" + k.getAs[Long](0) + "~" + k.getAs[String](1)).getBytes()
      // Column Family PersonID which composes of user_name, distinguished_name and display_name
      val userNameArr = (Bytes.toBytes("personID"), Bytes.toBytes("user_name"), k.getAs[String](2).getBytes())
      val distinguishedNameArr = (Bytes.toBytes("personID"), Bytes.toBytes("distinguished_name"), k.getAs[String](3).getBytes())
      val displayNameArr = (Bytes.toBytes("personID"), Bytes.toBytes("display_name"), k.getAs[String](4).getBytes())
      // Column Family MachineID
      val macAddrArr = (Bytes.toBytes("machineID"), Bytes.toBytes("mac_addr"), k.getAs[String](5).getBytes())
      val machineNameArr = (Bytes.toBytes("machineID"), Bytes.toBytes("machine_name"), k.getAs[String](6).getBytes())

      HbaseTabPutFormat(hbaseRowId, Array(userNameArr, distinguishedNameArr, displayNameArr, macAddrArr, machineNameArr))
    })
  }


  def writeToHbase(hbc: HBaseContext, hbTablename: String, rdd: RDD[HbaseTabPutFormat]) = {
    hbc.bulkPut[HbaseTabPutFormat](rdd,
      hbTablename,
      (putRecord) => {
        val put = new Put(putRecord.empIdDomainName)
        putRecord.colFamily.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
        put
      },
      true)
  }

  def main(args: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

    if (args.length < 3) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: StartDnsSparkFlumeAnalyzer <host> <port>  <HBase_tableName>")
      System.exit(1)
    } else {
      try {
        Some {
          val host = args(0)               // hostname or IP of Spark Sink host
          val port = args(1).toInt         // port that listens for Spark connection; in this case 22222
          val hbTableName = args(2)
        }
      }catch {
        case e: NumberFormatException => None
        }
    }


    //val Array(host, IntParam(port), kaceFile, nedFile, hbTableName) = args
    val Array(host, IntParam(port), hbTableName) = args

    val batchInterval = Milliseconds(2000)

    val DnsParser = new DnsLogParser()

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("DnsLogProcessing")
    val sc = new SparkContext(sparkConf)

    // Create HiveContext
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._

    // Create HBaseContext
    val HbaseConf = HBaseConfiguration.create()
    HbaseConf.addResource("/opt/app/spark/core-site.xml")
    HbaseConf.addResource("/opt/app/spark/hbase-site.xml")
    val hbaseContext = new HBaseContext(sc, HbaseConf);

    // Load and cache Kace data
    hiveContext.cacheTable("kace")

    // Load and cache Active Directory data
    hiveContext.cacheTable("active_directory")

    // Start streaming context
    val ssc = new StreamingContext(sc, batchInterval)

    // Check arguments passed in
    // System.out.println("host = " + host)
    // System.out.println("port = " + port)
    // System.out.println("hbaseTable = " + hbTableName)

    // Create a flume stream
    // val stream = FlumeUtils.createPollingStream(ssc, host, "22222".toInt, StorageLevel.MEMORY_ONLY_SER_2)
       val stream = FlumeUtils.createPollingStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)

    // Parse record
    val parsedDnsBatch = stream.map(record => DnsParser.nullPatternMatch(new String(record.event.getBody().array())))

    // parsedDnsBatch.foreachRDD(rdd => rdd.collect().foreach(println))


    parsedDnsBatch.foreachRDD(rdd => {
      if(rdd.count() > 0) {
        val dnsDataFrame = convertDnsBatchRecsToDFs(hiveContext, rdd)
        dnsDataFrame.registerTempTable("dnsDataFrame")

        // hiveContext.sql("SELECT * FROM dnsDataFrame").collect().foreach(println)

        val domainByIp = hiveContext.sql("SELECT a.clientIp, b.mac_addr, b.machine_name, b.user_name, a.domain, a.dateStamp, a.timeStamp FROM dnsDataFrame a JOIN kace b ON (a.clientIp = b.client_ip)")
        domainByIp.registerTempTable("domainByIp")

        //hiveContext.sql("SELECT * FROM domainByIp").collect().foreach(println)

        val domainByPerson = hiveContext.sql("SELECT b.emp_num, a.domain,  b.user_name, b.distinguished_name, b.display_name, a.mac_addr, a.machine_name, year, month, day FROM domainByIp a JOIN active_directory b ON (a.user_name = b.user_name)")
        domainByPerson.registerTempTable("domainByPerson")

        // hiveContext.sql("SELECT * FROM domainByPerson").collect().foreach(println)

        writeToHbase(hbaseContext, hbTableName, ConvertToHbasePut(domainByPerson) )

      }
    })


    ssc.start()
    ssc.awaitTermination()

  }
}
