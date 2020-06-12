import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object Test {

  case class UserItem(userId:String,itemId:String,score:Double)
  def main(args: Array[String]): Unit = {

    /**
    基于用户的协同过滤有以下几个缺点：
    1. 如果用户量很大，计算量就很大
    2. 用户-物品打分矩阵是一个非常非常非常稀疏的矩阵，会面临大量的null值
    很难得到两个用户的相似性
    3. 会将整个用户-物品打分矩阵都载入到内存，而往往这个用户-物品打分矩阵是一个
    非常大的矩阵

    所以通常不太建议使用基于用户的协同过滤

      **/

    //导入数据
    val spark = SparkSession.builder()
      .master("local")
        .config("spark.driver.memory","571859200")
      .config("spark.testing.memory","2147480000")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", true)
      .option("delimiter", ",")
      .option("inferschema", true)
      .schema(ScalaReflection.schemaFor[UserItem]
        .dataType.asInstanceOf[StructType])
      .load("H:\\BaiduNetdiskDownload\\Spark2.x+协同过滤算法，开发企业级个性化推荐系统\\431\\chapter_4\\cf_user_based.csv")

    df.show(false)

    // 通过余弦相似度计算用户的相似度
    // 余弦相似度的公式 ： (A * B) / (|A| * |B|)

    // 分母 每个向量的模的乘积
    import spark.implicits._
    val dfScoreMod = df.rdd.map(x=>(x(0).toString,x(2).toString))
      .groupByKey() //按照用户id进行分组
      .mapValues(score=>math.sqrt(
      score.toArray.map(
        itemScore=>math.pow(itemScore.toDouble,2)
      ).reduce(_+_)
      // ((物品a的打分)**2 + (物品b的打分)**2 .. (物品n的打分)**2))** 1/2
    ))
      .toDF("userId","mod")

    dfScoreMod.show(false)

    //分子
    val _dfTmp = df.select(
      col("userId").as("_userId"),
      col("itemId"),
      col("score").as("_score")
    )

    //这里会引起shuffle,大家思考下如何优化

    //这里目的是把两两用户都放到了同一张表里
    val _df = df.join(_dfTmp,df("itemId") === _dfTmp("itemId"))
      .where(
        df("userId") =!= _dfTmp("_userId")
      )
      .select(
        df("itemId"),
        df("userId"),
        _dfTmp("_userId"),
        df("score"),
        _dfTmp("_score")
      )

    _df.show(false)

    //  两两向量的维度乘积并加总

    val df_mol = _df.select(
      col("userId"),
      col("_userId"),
      (col("score") * col("_score"))
        .as("score_mol") //用户a和用户b对各自对同一个物品打分的乘积
    ).groupBy(col("userId"),col("_userId"))
      .agg(sum("score_mol"))  //加总
      .withColumnRenamed(
      "sum(score_mol)",
      "mol"
    )

    df_mol.show(false)

    // 计算两两向量的余弦相似度

    val _dfScoreMod = dfScoreMod.select(
      col("userId").as("_userId"),
      col("mod").as("_mod")
    )

    //这里也会引起shuffle,大家思考下如何优化

    //分子表(df_mol)和分母表(dfScoreMod)都放在一张表里
    val sim =  df_mol.join(
      dfScoreMod,
      df_mol("userId") === dfScoreMod("userId")
    ).join(
      _dfScoreMod,
      df_mol("_userId") === _dfScoreMod("_userId")
    ).select(
      df_mol("userId"),
      df_mol("_userId"),
      df_mol("mol"),
      dfScoreMod("mod"),
      _dfScoreMod("_mod")
    )

    sim.show(false)

    // 分子 / 分母
    val cos_sim = sim.select(
      col("userId"),
      col("_userId"),
      (col("mol") /
        (col("mod") * col("_mod")))
        .as("cos_sim")
    )

    cos_sim.show(false)

    // 列出某个用户的TopN相似用户

    val topN = cos_sim.rdd.map(x=>(
      (x(0).toString,
        (x(1).toString,x(2).toString)
      )  // 形成 (uid1,(uid2,cos_sim))
      )).groupByKey()
      .mapValues(_.toArray.sortWith((x,y)=>x._2 > y._2)) //根据相似度排序
      .flatMapValues(x=>x)
      .toDF("userId","sim_sort")
      .select(
        col("userId"),
        col("sim_sort._1").as("_userId"),
        col("sim_sort._2").as("cos_sim")
      ).where(col("userId") === "1")

    topN.show(false)



  }

}
