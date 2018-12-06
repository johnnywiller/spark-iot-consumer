package spark_consumer

import java.lang
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import play.api.libs.json.Json
import play.api.libs.json.Json.reads


case class Location(latitude: Double, longitude: Double)
case class Data(deviceId: String, var temperature: Int = 0, var location: Location = null, var time: Instant = Instant.now())
case class DataParameters(maxTemperature: Int, minTemperature: Int, deltaTemperature: Int, maxLatitude: Double, minLatitude: Double, maxLongitude: Double, minLongitude: Double, deltaLocation: Double)

object DataConsumer {

    var conn: Connection = _

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("IoT Data Conusmer").setMaster("local[*]")

        val ssc = new StreamingContext(conf, Milliseconds(20))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "iotconsumer",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: lang.Boolean)
        )

        val topics = Array("iot-topic")
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        bootstrapHBase()

        stream.map(record => (record.value)).foreachRDD(rdd => {
            rdd.foreach(data => {
                insertHBase("iot_metric", data)
            })
        })

        ssc.start()
        ssc.awaitTermination()

    }


    def insertHBase(table: String, data: String) = {

        implicit val locationReads = reads[Location]
        implicit val dataReads = reads[Data]

        val _table = getConnection.getTable(TableName.valueOf(table))

        val _data = (Json.parse(data) \ "data").as[Data]

        val df : SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")

        // setting de rowKey as deviceId seems to be a good choice due the nature of HBase
        val put = new Put(Bytes.toBytes(_data.deviceId + df.format(new Date(_data.time.toEpochMilli))))

        put.addColumn(Bytes.toBytes("rawInfo"), Bytes.toBytes("raw"), Bytes.toBytes(data))

        put.addColumn(Bytes.toBytes("deviceInfo"), Bytes.toBytes("devid"), Bytes.toBytes(_data.deviceId))
        put.addColumn(Bytes.toBytes("deviceInfo"), Bytes.toBytes("temp"), Bytes.toBytes(_data.temperature))
        put.addColumn(Bytes.toBytes("deviceInfo"), Bytes.toBytes("lat"), Bytes.toBytes(_data.location.latitude))
        put.addColumn(Bytes.toBytes("deviceInfo"), Bytes.toBytes("long"), Bytes.toBytes(_data.location.longitude))
        put.addColumn(Bytes.toBytes("deviceInfo"), Bytes.toBytes("time"), Bytes.toBytes(df.format(new Date(_data.time.toEpochMilli))))

        _table.put(put)

    }

    def configHBase(): Configuration = {

        // simple config, using Dockerized HBase
        val config = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", "quickstart.cloudera")

        return config

    }

    def bootstrapHBase() = {

        // if HBase isn't available, don't proceed
        HBaseAdmin.checkHBaseAvailable(configHBase())

        val admin = getConnection.getAdmin

        if (!admin.tableExists(TableName.valueOf("iot_metric"))) {

            val tableDescriptor : HTableDescriptor = new HTableDescriptor(TableName.valueOf("iot_metric"));

            tableDescriptor.addFamily(new HColumnDescriptor("deviceInfo"))
            tableDescriptor.addFamily(new HColumnDescriptor("rawInfo"))

            admin.createTable(tableDescriptor)
        }
    }

    def getConnection: Connection = {
        if (conn == null)
            conn = ConnectionFactory.createConnection(configHBase())

        conn
    }

}
