package com.bfd.api.streaming.scala

import java.sql.{Connection, ResultSet}
import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory
import java.util.Properties
import java.io.FileInputStream
import org.apache.log4j.PropertyConfigurator;

object ConnectionPool {

  val logger = LoggerFactory.getLogger("info")
  val errorlog = LoggerFactory.getLogger("error")
  private val connectionPool = {
    try{
      
      // 加载server配置文件
		val osName = System.getProperties().getProperty("os.name")
//		var path = "resources/mysql-conf.properties"
//    var path = "/Users/jiangnan/Documents/work/baifendian/BD-OSV3.0/bdos_project/api-scala/src/mysql-conf.properties"

    if(osName.indexOf("linux") >= 0 || osName.indexOf("Linux") >= 0 || osName.indexOf("LINUX") >= 0){
//			path = "mysql-conf.properties";
			
		}
    
//    val properties = new Properties()
//    properties.load(new FileInputStream(path))
    var url = "jdbc:mysql://172.24.5.158:3306/test?characterEncoding=utf-8"// jdbc:mysql://172.24.5.125:3306/shenzhengxin_2?characterEncoding=utf-8

    logger.info("url=" + url)
      
    var userName = "root"
    logger.info("userName=" + userName)
    
     var passWord = "root"
    logger.info("passWord=" + passWord)
    
    
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl(url)
//      config.setUsername(SzxEncrypt.decrypt(userName))
      config.setUsername(userName)
//      config.setPassword(SzxEncrypt.decrypt(passWord))
      config.setPassword(passWord)
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)

      Some(new BoneCP(config))
    } catch {
      case exception:Exception=>
        errorlog.error("Error in creation of connection pool"+exception.printStackTrace())
        None
    }
  }
  def getConnection:Option[Connection] ={
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }
  def closeConnection(connection:Connection): Unit = {
    if(!connection.isClosed) {
      connection.close()

    }
  }
}