package com.qf.bigdata.util

object formatType {
    // 转换Int类型
    def str2Int(str:String):Int ={
        try{
            Integer.valueOf(str)
        }catch {
            case e:Exception=> 0
        }
    }
    // 转换Double类型
    def str2Double(str:String):Double ={
        try{
            str.toDouble
        }catch {
            case e:Exception=> 0.0
        }
    }
}
