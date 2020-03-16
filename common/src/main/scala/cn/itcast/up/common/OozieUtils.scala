package cn.itcast.up.common

import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.mapreduce.v2.api.records.JobId
import org.apache.oozie.client.{AuthOozieClient, OozieClient}

object OozieUtils {

  val classLoader: ClassLoader = getClass.getClassLoader

  /**
    * Properties 包含各种配置
    * OozieParam 外部传进来的参数
    * 作用: 生成配置, 有些配置无法写死, 所以外部传入
    */
  def genProperties(param: OozieParam): Properties = {
    val properties = new Properties()

    val params: Map[String, String] = ConfigHolder.oozie.params
    for (entry <- params) {
      properties.setProperty(entry._1, entry._2)
    }

    val appPath = ConfigHolder.hadoop.nameNode + genAppPath(param.modelId)
    properties.setProperty("appPath", appPath)

    properties.setProperty("mainClass", param.mainClass)
    properties.setProperty("jarPath", param.jarPath)

    if (StringUtils.isNotBlank(param.sparkOptions)) {
      properties.setProperty("sparkOptions", param.sparkOptions)
    }
    properties.setProperty("start", param.start)
    properties.setProperty("end", param.end)
    properties.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath)

    properties
  }

  /**
    * 上传配置
    * @param modelId 因为要上传到 家目录, 所以要传入 id 生成家目录
    */
  def uploadConfig(modelId: Long): Unit = {
    val workflowFile = classLoader.getResource("oozie/workflow.xml").getPath
    val coordinatorFile = classLoader.getResource("oozie/coordinator.xml").getPath

    val path = genAppPath(modelId)
    HDFSUtils.getInstance().mkdir(path)
    HDFSUtils.getInstance().copyFromFile(workflowFile, path + "/workflow.xml")
    HDFSUtils.getInstance().copyFromFile(coordinatorFile, path + "/coordinator.xml")
  }

  def genAppPath(modelId: Long): String = {
    ConfigHolder.model.path.modelBase + "/tags_" + modelId
  }

  def store(modelId: Long, prop: Properties): Unit = {
    val appPath = genAppPath(modelId)
    prop.store(HDFSUtils.getInstance().createFile(appPath + "/job.properties"), "")
  }

  def start(prop: Properties): String = {
    val oozie: OozieClient = new OozieClient(ConfigHolder.oozie.url)
    println(prop)
    val jobId = oozie.run(prop)
    println(jobId)
    jobId
  }

  def stop(jobId:String) :Unit ={
    //val oozie: OozieClient = new OozieClient(ConfigHolder.oozie.url)
    System.setProperty("user.name", "root")
    val oozie: AuthOozieClient = new AuthOozieClient(ConfigHolder.oozie.url,AuthOozieClient.AuthType.KERBEROS.name())
    //val oozie: AuthOozieClient = new AuthOozieClient(ConfigHolder.oozie.url,"root")
    oozie.kill(jobId)
  }

  /**
    * 工具类测试
    */
  def main(args: Array[String]): Unit = {
    val param = OozieParam(
      30,
      "cn.itcast.up29.TestTag",
      "hdfs://bd001:8020/apps/tags/models/Tag_001/lib/model29.jar",
      "",
      "2019-09-24T06:15+0800",
      "2030-09-30T06:15+0800"
    )
    val prop = genProperties(param)
    println(prop)
    uploadConfig(param.modelId)
    store(param.modelId, prop)
    start(prop)
  }
}

case class OozieParam
(
  modelId: Long,
  mainClass: String,
  jarPath: String,
  sparkOptions: String,
  start: String,
  end: String
)
