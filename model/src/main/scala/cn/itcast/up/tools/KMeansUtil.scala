package cn.itcast.up.tools

import cn.itcast.up.common.HDFSUtils
import cn.itcast.up.ml.PSMModel.spark
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.DataFrame

import scala.collection.immutable

/**
 * Author itcast
 * Date 2020/2/28 11:35
 * Desc 
 */
object KMeansUtil {
  def getIndex2TagIdMap(model:KMeansModel,fiveDF:DataFrame)={
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //先获取聚类中心编号和聚类中心RFE的sum,并按照FRE的sum排序
    //IndexedSeq[(聚类中心编号, 聚类中心RFE的sum)]
    val sortedIndexAndRFESum: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(index => {
      (index, model.clusterCenters(index).toArray.sum)
    }).sortBy(_._2)
    //下面的yield叫做列表生成式和上的结果等价
    //val indexAndRFESum2: immutable.IndexedSeq[(Int, Double)] = for(index <- model.clusterCenters.indices) yield (index,model.clusterCenters(index).toArray.sum)

    //再将用户活跃度标签id和用户等级关系排好序
    //List[(标签id, 用户等级)]
    val sortedTagIdAndRule: List[(Long, String)] = fiveDF.as[(Long, String)].map(t => (t._1, t._2)).collect().toList.sortBy(_._2)

    //将上面两个排好序的集合进行合并/拉链得出聚类中心编号对应的用户活跃度等级id/标签id
    //[((聚类中心编号, 聚类中心RFE的sum), (标签id, 用户等级))]
    val zipResult: immutable.IndexedSeq[((Int, Double), (Long, String))] = sortedIndexAndRFESum.zip(sortedTagIdAndRule)

    //Map[聚类中心编号, 5级标签id]
    val index2TagIdMap: Map[Int, Long] = zipResult.map(t => {
      (t._1._1, t._2._1)
    }).toMap
    index2TagIdMap
  }

  def getModel(path:String,k:Int,featureStr:String,predictStr:String,vectorDF:DataFrame):KMeansModel ={
    //声明模型
    var model: KMeansModel = null
    //判断模型是否存在
    if (HDFSUtils.getInstance().exists(path)) {
      println("模型存在直接加载并使用")
      model = KMeansModel.load(path)
    } else {
      println("模型不存在,先训练再保存")
      model = new KMeans()
        .setK(k)
        .setInitSteps(10)
        .setSeed(100)
        .setFeaturesCol(featureStr)
        .setPredictionCol(predictStr)
        .fit(vectorDF)
      model.save(path)
    }
    model
  }

  def selectK(featureStr: String, predictStr: String, vectorDF: DataFrame, ks: List[Int]): Int = {
    /*//准备集合用来存放k值和对应的SSE
    val map: mutable.Map[Int, Double] = mutable.Map[Int, Double]()
    //计算每一个k值对应的SSE
    for (k <- ks) {
      val model: KMeansModel = new KMeans()
        .setK(k)
        .setInitSteps(10)
        .setSeed(100)
        .setFeaturesCol(featureStr)
        .setPredictionCol(predictStr)
        .fit(vectorDF)
      val sse: Double = model.computeCost(vectorDF)
      map.put(k, sse)
    }
    println("k值对应的SSE的值为:")
    map.foreach(println)*/
    //假设选取的k值为5
    5
  }
}
