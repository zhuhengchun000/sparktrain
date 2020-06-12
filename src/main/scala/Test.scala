//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.ScalaReflection
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.functions._
//
//object Test {
//
//  case class UserItemVote(userId:String,itemId:String,vote:Float)
//  def main(args: Array[String]): Unit = {
//
//      val spark = SparkSession.builder()
//        .master("local")
//        .getOrCreate()
//
//      val userItemData = spark.read.format("csv")
//      .option("header",true)
//      .option("delimiter",",")
//      .option("inferSchema",true)
//      .schema(ScalaReflection.schemaFor[UserItemVote]
//      .dataType.asInstanceOf[StructType])
//      .load("C:\\imooc\\cf_item_based.csv")
//
////      userItemData.show(false)
//
//    // 用户-物品打分表(矩阵)
//     val userItemTmp = userItemData.groupBy(col("userId")).pivot(col("itemId"))
//      .sum("vote")
//
////     userItemTmp.show(false)
//
//    // 物品相似度表 (矩阵)
//
//    val rddTmp = userItemData.rdd.filter(x=>x.getFloat(2) > 0)
//      .map(x=>{
//        (x.getString(0),x.getString(1))
//      })
//
//    // (userid,(itemid_1,itemId_2))
//    import spark.implicits._
//    val itemSim = rddTmp.join(rddTmp)
//      .map(x=>(x._2,1))
//      .reduceByKey(_+_)
//      .filter(x=>x._1._1 != x._1._2)
//      .map(x=>{
//        (x._1._1,x._1._2,x._2)
//      })
//      .toDF("itemId_1","itemId_2","sim")
//
////     itemSim.show(false)
//
//     val itemSimTmp = itemSim.groupBy(col("itemId_1"))
//      .pivot(col("itemId_2"))
//      .sum("sim")
//      .withColumnRenamed(
//        "itemId_1",
//        "itemId"
//      ).na.fill(0)
//
////     itemSimTmp.show(false)
//
//     // 用户-物品打分矩阵 * 物品相似度矩阵 = 推荐列表
//
//    val itemInterest = userItemData.join(
//      itemSim,
//      itemSim("itemId_2") === userItemData("itemId")
//    ).where(col("userId") === "User_A")
//      .select(
//        userItemData("userId"),
//        userItemData("itemId"),
//        userItemData("vote"),
//        itemSim("itemId_1"),
//        itemSim("itemId_2"),
//        itemSim("sim"),
//        (userItemData("vote") * itemSim("sim"))
//          .as("interest")
//      )
//
////    itemInterest.show(false)
//
//    val interestList = itemInterest.groupBy(col("itemId_1"))
//      .agg(sum(col("interest")))
//      .withColumnRenamed(
//        "sum(interest)",
//        "interest"
//      )
//      .orderBy(desc("interest"))
//
//     interestList.show(false)
//
//  }
//
//}
