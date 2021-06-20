package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.functions.{col, explode, udf,from_json,collect_list,struct}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object scalaObj {
	def main(args:Array[String]):Unit={


			val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._				

					//spark.read.schema(schema).json(file).filter($"_corrupt_record".isNotNull).count()

					//spark.read.schema(schema).json(file).select("_corrupt_record").show().
					/*
					println("============================Raw Data ===============================================")
					val rawdf=spark.read.option("multiLine","true").json("file:///c:/data/jsondata/arrayjson.json")		
					rawdf.show()
					rawdf.printSchema()

					println("============================Flatten Raw Data ===============================================")
					//val rawdf=spark.read.option("multiLine","true").json("file:///c:/data/jsondata/arrayjson.json")		
					//rawdf.show()
					//rawdf.printSchema()

					 */
					println("========================raw df==================")

					val rawdf = spark.read.format("json").option("multiLine","true")
					.load("file:///C://data//complexjson//arrayjson.json")

					rawdf.show()
					rawdf.printSchema()

					println("====================flatten df==========")


					val flattendf = rawdf.withColumn("Students", explode(col("Students")))
					.select(

							col("Students.gender"),
							col("Students.name"),
							col("address.Permanent_address"),
							col("address.temporary_address"),
							col("first_name"),
							col("second_name")

							)

					flattendf.show()
					flattendf.printSchema()


					val complexdf=	flattendf.
					groupBy("Permanent_address","temporary_address","first_name","second_name")

					.agg(collect_list(

							struct(

									col("gender"),
									col("name")

									)


							).alias("Students")
							)

					val finaldf = complexdf.select(
							col("Students"),
							struct(

									col("Permanent_address"),
									col("temporary_address")

									).alias("address")

							,col("first_name")
							,col("second_name")

							)




					complexdf.show()
					complexdf.printSchema()


	}

}