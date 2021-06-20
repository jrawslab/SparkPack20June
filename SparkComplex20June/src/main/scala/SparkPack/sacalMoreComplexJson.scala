package SparkPack
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.functions.{col, explode, udf,from_json,collect_list,struct,array}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object sacalMoreComplexJson {
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
					val rawdf = spark.read.format("json").option("multiLine","true")
					.load("file:///C://data//complexjson//MultiArrays.json")

					rawdf.show(false)
					rawdf.printSchema()


					println("==========================flatten df=================")



					val addressdf = rawdf.select(
							col("Students"),
							col("address.Permanent_address"),
							col("address.temporary_address"),
							col("first_name"),
							col("second_name")
							)


					addressdf.show()
					addressdf.printSchema()


					val arraydf = addressdf.

					withColumn("Students",explode(col("Students")))
					.withColumn("components",explode(col("Students.user.components")))

					arraydf.printSchema()
					arraydf.show()

					val finaldf1 = arraydf

					.select(

							col("Students.user.address.Permanent_address").alias("cPermanent_address"),
							col("Students.user.address.temporary_address").alias("ctemporary_address"),
							col("Students.user.gender"),
							col("Students.user.name.first"),
							col("Students.user.name.last"),
							col("Students.user.name.title"),
							col("Permanent_address"),
							col("temporary_address"),
							col("first_name"),
							col("second_name"),
							col("components")

							)
					finaldf1.show()
					finaldf1.printSchema()


					println("==========================complex df=================")

					val complexdf = finaldf1.groupBy("Permanent_address","temporary_address","first_name","second_name")
					.agg(

							collect_list(


									struct(

											struct(

													struct(
															col("cPermanent_address").alias("Permanent_address"),
															col("ctemporary_address").alias("temporary_address")
															).alias("address"),

													array("components").alias("components"),

													col("gender"),
													struct(
															col("first"),
															col("last"),
															col("title")

															).alias("name")




													).alias("user")

											)

									).alias("Students")	

							)




					complexdf.show()
					complexdf.printSchema()



					val finaldf = complexdf.select(



							col("Students"),
							struct(

									col("Permanent_address"),
									col("temporary_address")

									).alias("address")

							,col("first_name")
							,col("second_name")


							)


					finaldf.show(false)
					finaldf.printSchema()

	}

}