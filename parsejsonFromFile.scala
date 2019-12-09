import scala.util.parsing.json._
import scala.io.Source
import scala.collection.mutable.ListBuffer

object parsejsonFromFile {
  def getSqoopCommands(filePath : String): List[String] = {
    var sqoopCommands = new ListBuffer[String]()
    val rawData = Source.fromFile(filePath).getLines.mkString
    println(rawData)
    val result = JSON.parseFull(rawData)
    val resultMap = result.get.asInstanceOf[Map[String, Any]]
    val name_sqoop = resultMap.get("sqoop").get.asInstanceOf[Map[String, Any]]

    //----------------Details---------------------------------------//
    val name_details = name_sqoop.get("details").get.asInstanceOf[Map[String,Any]]
    val value_num_mappers= name_details.get("num_mappers").get.asInstanceOf[String]
    val value_compression= name_details.get("compression").get.asInstanceOf[String]
    //----------------Details---------------------------------------//

    val name_import = name_sqoop.get("import").get.asInstanceOf[List[Map[String,Any]]]

    println(resultMap)
    println(name_sqoop)
    println(name_import)
    println(name_details)
    println("number of mapper = "+ value_num_mappers)
    println("compression = "+ value_compression)

    for(i <-  name_import){
      val value_source_table = i.get("source_table").get.asInstanceOf[String]
      val value_target_type = i.get("target_type").get.asInstanceOf[String]
      val value_target_path = i.get("target_path").get.asInstanceOf[String]
      val value_file_type = i.get("file_type").get.asInstanceOf[String]

      println("value_source_table : " + value_source_table)
      println("value_target_type  : " + value_target_type)
      println("value_target_path  : " + value_target_path)
      println("value_file_type    : " + value_file_type)
      sqoopCommands +=
        """
          sqoop import --connect jdbc:mysql://localhost/classicmodels --username root --password cloudera
          --table %s --target-dir %s --as-%s
        """.format(value_source_table,value_target_path,value_file_type)
    }
    val sqoopCommandList = sqoopCommands.toList
    return sqoopCommandList
  }

  def main(args: Array[String]): Unit = {
    val filePath = "D:\\data\\sqoop_prop_json.properties"
    val sqoopCommandsList = getSqoopCommands(filePath)
    for(i <- sqoopCommandsList){
      print(i)
    }
  }
}
