package io.delta.kernel.defaults.ccv2.setup

import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.utils.FileStatus

/////////////////////////////////////////////////////////////////
// CatalogClient interactions -- no direct contact with Kernel //
/////////////////////////////////////////////////////////////////

trait CatalogClient {
  def createStagingTable(tableName: String): CreateStagingTableResponse

  def getProperties(tableName: String): GetPropertiesResponse

  // e.g. COMMIT --> add new properties for UUID file status, protocol, metadata
  // e.g. SET LAST BACKFILLED VERSION --> ???? remove all properties which we parse and determine
  //      the version is less than the incoming version ???
  def setProperties(
      tableName: String,
      properties: List[(String, String)],
      requirements: List[Requirement] = List.empty
  ): SetPropertiesResponse

}

case class Requirement(name: String, f: Map[String, String] => Boolean)

// ===== CreateStagingTableResponse ===== //

sealed trait CreateStagingTableResponse

object CreateStagingTableResponse {
  final case class Success(path: String) extends CreateStagingTableResponse

  final case object TableAlreadyExists extends CreateStagingTableResponse
}

// ===== GetPropertiesResponse ===== //

sealed trait GetPropertiesResponse

object GetPropertiesResponse {
  final case class Success(properties: List[(String, String)]) extends GetPropertiesResponse

  final case object TableDoesNotExist extends GetPropertiesResponse
}

// ===== SetPropertiesResponse ===== //

sealed trait SetPropertiesResponse

object SetPropertiesResponse {
  final case object Success extends SetPropertiesResponse

  final case object TableDoesNotExist extends SetPropertiesResponse

  final case class RequirementFailed(
      requirement: Requirement,
      properties: List[(String, String)]) extends SetPropertiesResponse
}
