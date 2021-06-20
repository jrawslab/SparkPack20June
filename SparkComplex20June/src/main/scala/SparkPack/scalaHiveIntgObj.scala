package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.functions.{col, explode, udf,from_json,collect_list,struct}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object scalaHiveIntgObj {
  	def main(args:Array[String]):Unit={


			val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._				

}
}

 

 
