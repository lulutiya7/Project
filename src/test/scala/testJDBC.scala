
import java.util.Properties

import com.qf.bigdata.util.{JDBC_LoadAndWrite, LoggerKIiller}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object testJDBC {
    def main(args: Array[String]): Unit = {
        LoggerKIiller.killer()
        val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
        //JDBC_LoadAndWrite.readFromMysql(spark,"city","area").show()

        JDBC_LoadAndWrite.saveToMysql(spark,"output/rs","city","area")


    }

}
