package com.mmdata.wk

import java.net.URL
import java.util.HashSet

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

 object SimpleDeal {
   def main(args: Array[String]) {
     
     val conf = new SparkConf()
     conf.setAppName("SimpleDeal")
     conf.setMaster("local") //本地测试
     val sc = new SparkContext(conf)
     //val line = sc.textFile(args(0));
     val line = sc.textFile("d://sparkFile/log.txt");
     //相当于mapreduce中的map，找出合适key的输出         
     /**
      * line.filter(a,b)
      * a相当于mapreduce中map中输出中的key,做排重使用的，b相当于mapreduce中reduce做计算使用的。
      */
     val phone = line.filter(li => li.split("\\|").length == 6).map{li => (getDomian(li.split("\\|")(5).trim())+"_"+getSubmain(li.split("\\|")(5).trim())+"_"+getDate(li.split("\\|")(4))+"|",li.split("\\|")(0).trim())}
     //val phone = line.map{line => (getDomian(line.split("\\|")(5))+"_"+getSubmain(line.split("\\|")(5))+"_"+getDate(line.split("\\|")(4)),line.split("\\|")(0))}
     val words = phone.reduceByKey((x,y) => getphone(x+","+y))//相当于mapreduce中的reduce
     //val re = words.collect().foreach(resu => println(resu._1+""+resu._2.split(",").length))
     //re.saveAsTextFile(args(1))
     //words.saveAsTextFile(args(1))
     /**
      * 遍历word的值，
      * word.collect().foreach()
      * resu => println(resu._1+""+resu._2.split(",").length)
      * resu._1 输出的是reduce中的key,resu._2 输出的是reduce中的val
      */
     words.collect().foreach(resu => println(resu._1+""+resu._2.split(",").length))
   }
   
   
    def getphone(phone : String) : String ={
    
      val set : HashSet[String]  =new HashSet[String]();
      var resu = ""
      val fields = phone.split(",")
      for(i <- 0.to(fields.length-1)){
          set.add(fields.apply(i))
      }
      val ite = set.iterator()
      while(ite.hasNext()){
        resu +=ite.next()+","
      }
      if(resu.startsWith(",")){
        resu = resu.substring(1,resu.length())
      }
      if(resu.endsWith(",")){
         resu = resu.substring(0,resu.length()-1)
      }
      return resu
    }
    
     def getDomian(url : String) : String ={
          var url_ : String = ""
          if(!url.startsWith("http")){
                 url_ = "http://".concat(url)
          }else if(url.startsWith("http")){
            url_ = url
          }
          val host = new URL(url_).getHost.toLowerCase()
          val numPattern = "[^\\.]+(\\.com\\.cn|\\.net\\.cn|\\.org\\.cn|\\.gov\\.cn|\\.com|\\.net|\\.cn|\\.org|\\.tel|\\.mobi|\\.asia|\\.biz|\\.info|\\.name|\\.tv|\\.hk|\\.公司|\\.中国|\\.网络)".r
          return numPattern.findFirstIn(host).mkString
     }
     
     def getSubmain(url : String) : String = {
          var url_ : String = ""
          if(!url.startsWith("http://")){
                 url_ = "http://".concat(url)
          }else{
                 url_ = url
          }
          val host = new URL(url_).getHost.toLowerCase()
        return host
     }
     
     
     
     def getDate(date:String) : String = {
       val da = date.replaceAll("-", "")
       val dateresu = da.substring(0,8)
       return dateresu
     }
 }