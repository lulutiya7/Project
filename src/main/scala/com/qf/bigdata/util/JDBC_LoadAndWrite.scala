package com.qf.bigdata.util
import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SaveMode, SparkSession}

object JDBC_LoadAndWrite {
     var JDBCURL = "jdbc:mysql://localhost/"+DATABASE_NAME+"?useUnicode=true&characterEncoding=utf8"
     var URL = "jdbc:mysql://localhost:3306/"
     var DATABASE_NAME = ""
     var DBTABLE_NAME = ""
     val USER = "root"
     val PASSWORD = "123456"

    def saveToMysql(spark: SparkSession,readPath:String,Database_Name: String,Table_Name:String) = {
    DBTABLE_NAME = Table_Name
    DATABASE_NAME = Database_Name

    val dataframe: DataFrame = spark.read.parquet(readPath)

    val df: DataFrame = dataframe.groupBy("provincename", "cityname").count()
        df.show()

    val prop = new Properties()
    prop.setProperty("user", USER)
    prop.setProperty("password", PASSWORD)

    /**
     * 可以指定保存模式
     * SaveMode.Append
     * SaveMode.ErrorIfExists 默认的,已有的表会报错
     * SaveMode.Ignore  如果数据存在,就不保存df的数据
     * SaveMode.Overwrite 删除原有表重新建立
     */
    //将数据写入MySQL
     //df.write.mode(SaveMode.Overwrite).jdbc(URL, DBTABLE_NAME, prop)
     val conn = () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection(JDBCURL, USER, PASSWORD)
    }
//    val showInfo:JdbcRDD[(Int, String, Int, Date)] = new JdbcRDD()(spark.conf, conn, sql, 0, 10000, 1, (res: ResultSet) => {
//        val empno: Int = res.getInt("empno")
//        val ename: String = res.getString("ename")
//        val mgr: Int = res.getInt("mgr")
//        val hiredate: Date = res.getDate("hiredate")
//        (empno, ename, mgr, hiredate)
//    })
}

    def readFromMysql(spark: SparkSession,Database_Name: String,Table_Name:String) = {
        URL = URL + Database_Name
        DBTABLE_NAME = Table_Name
        val prop = new Properties()
           prop.setProperty("user", USER)
           prop.setProperty("password", PASSWORD)
        //从MySQL读取数据
        //val reader: DataFrameReader = spark.read.format("jdbc").option("url", URL + DATABASE_NAME).option("dbtable", DBTABLE_NAME).option("user", USER).option("password", PASSWORD)
        val frame: DataFrame = spark.read.jdbc(URL,DBTABLE_NAME, prop)
        frame
    }

}
