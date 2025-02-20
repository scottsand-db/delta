package io.delta.kernel.defaults.ccv2.setup

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import io.delta.kernel.internal.fs.Path

class InMemoryCatalogClient(workspace: Path = new Path("/tmp/in_memory_catalog/"))
  extends CatalogClient {
  import InMemoryCatalogClient._
  java.nio.file.Files.createDirectories(java.nio.file.Paths.get(workspace.toString))

  ///////////////////
  // Member values //
  ///////////////////

  case class CatalogTableData(properties: scala.collection.mutable.TreeMap[String, String])

  /** Map from tableName -> CatalogTableData */
  val catalogTables = new ConcurrentHashMap[String, CatalogTableData]()

  /** Map from tableName -> stagingTablePath */
  // TODO: support concurrent staging tables, probably using some UUID?
  val stagingTables = new ConcurrentHashMap[String, String]()

  ////////////////
  // Public API //
  ////////////////

  override def createStagingTable(tableName: String): CreateStagingTableResponse = {
    catalogTables.get(tableName) match {
      case null =>
        // TODO: support concurrent staging tables, probably using some UUID?
        val uuid = UUID.randomUUID().toString.replace("-", "").take(15)
        val tablePath = s"${workspace.toString}/${tableName}_$uuid"
        logger.info(s"CREATE STAGING TABLE :: NEW :: $tableName -> $tablePath")
        stagingTables.put(tableName, tablePath)
        CreateStagingTableResponse.Success(tablePath)
      case tableData =>
        logger.info(s"CREATE STAGING TABLE :: ALREADY EXISTS :: $tableName")
        CreateStagingTableResponse.TableAlreadyExists
    }
  }

  override def getProperties(tableName: String): GetPropertiesResponse = {
    catalogTables.get(tableName) match {
      case null =>
        logger.info(s"GET PROPERTIES > TABLE $tableName DOES NOT EXIST")
        GetPropertiesResponse.TableDoesNotExist
      case tableData =>
        logger.info(s"GET PROPERTIES > TABLE $tableName EXISTS")
        GetPropertiesResponse.Success(tableData.properties.toList)
    }
  }

  override def setProperties(
      tableName: String,
      properties: List[(String, String)],
      requirements: List[Requirement] = List.empty): SetPropertiesResponse = {
    catalogTables.get(tableName) match {
      case null =>
        logger.info(s"SET PROPERTIES > TABLE $tableName DOES NOT EXIST")
        stagingTables.get(tableName) match {
          case null =>
            logger.info(s"SET PROPERTIES > STAGING TABLE $tableName DOES NOT EXIST, EITHER")
            SetPropertiesResponse.TableDoesNotExist
          case stagingTablePath =>
            logger.info(s"SET PROPERTIES > WRITING TO STAGING TABLE $stagingTablePath")
            val data = CatalogTableData(scala.collection.mutable.TreeMap(properties: _*))
            catalogTables.put(tableName, data)
            SetPropertiesResponse.Success
        }
      case tableData =>
        logger.info(s"SET PROPERTIES > TABLE $tableName EXISTS")

        requirements.foreach { requirement =>
          requirement.f.apply(tableData.properties.toMap) match {
            case false =>
              logger.info(s"REQUIREMENT FAILED: ${requirement.name}")
              return SetPropertiesResponse.RequirementFailed(
                requirement, tableData.properties.toList)
            case true =>
              logger.info(s"REQUIREMENT SUCCEEDED: ${requirement.name}")
          }
        }

        properties.foreach {
          case (key, null) =>
            logger.info(s"REMOVE KEY $key. Key exists? ${tableData.properties.contains(key)}")
            tableData.properties -= key
          case (key, value) =>
            tableData.properties.update(key, value)
        }
        logger.info(s"PROPERTIES IS NOW:\n${tableData.properties.mkString("\n")}")
        SetPropertiesResponse.Success
    }
  }
}

object InMemoryCatalogClient {
  val logger = org.slf4j.LoggerFactory.getLogger(classOf[InMemoryCatalogClient])
}
