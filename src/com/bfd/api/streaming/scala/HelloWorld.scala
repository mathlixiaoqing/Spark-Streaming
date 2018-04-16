package com.bfd.api.streaming.scala

import java.util.Date
import java.text.SimpleDateFormat

/**
 * @author BFD_387
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("hello world")
    
    val daystr = "2017-03-1315:20:31";
//    val daystr = "20170313152031";
                System.out.println("strday=" + daystr.toString())
                val format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
                val format2 = new SimpleDateFormat("yyyy-MM-dd")
                val d = format.parse(daystr);
                
                System.out.println("d=" + d)
                val t = new Date(d.getTime());
                // 1489389631000
                 System.out.println("d2=" + d.getTime)
                System.out.println("dateday=" + t)
                
                val td = new java.sql.Date(d.getTime);
                 System.out.println("td=" + td)
                 
                
                val t2 = format2.format(t);
                System.out.println("dateday2=" + t2)
  }
}