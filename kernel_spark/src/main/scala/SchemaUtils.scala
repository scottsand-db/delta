import io.delta.kernel.types.{DataType => KernelDataType, StructField => KernelStructField, StructType => KernelStructType, StringType => KernelStringType, BooleanType => KernelBooleanType, IntegerType => KernelIntegerType, LongType => KernelLongType}

import org.apache.spark.sql.types.{DataType => SparkDataType, StructType => SparkStructType, StructField => SparkStructField, StringType => SparkStringType, BooleanType => SparkBooleanType, IntegerType => SparkIntegerType, LongType => SparkLongType}

import collection.JavaConverters._

object SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  def convertKernelSchemaToSparkSchema(kernelSchema: KernelStructType): SparkStructType = {
    SparkStructType(kernelSchema.fields().asScala.map { field =>
      SparkStructField(
        field.getName,
        convertKernelDataTypeToSparkDataType(field.getDataType),
        field.isNullable)
    })
  }

  def convertKernelDataTypeToSparkDataType(kernelDataType: KernelDataType): SparkDataType = {
    kernelDataType match {
      case _: KernelStringType => SparkStringType
      case _: KernelBooleanType => SparkBooleanType
      case _: KernelIntegerType => SparkIntegerType
      case _: KernelLongType => SparkLongType
      case x => throw new IllegalArgumentException(s"unsupported data type $x")
    }
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  def convertSparkSchemaToKernelSchema(sparkSchema: SparkStructType): KernelStructType = {
    new KernelStructType(
      sparkSchema.fields
        .map { field =>
          new KernelStructField(
            field.name,
            convertSparkDataTypeToKernelDataType(field.dataType),
            field.nullable)
        }
        .toList
        .asJava)
  }

  def convertSparkDataTypeToKernelDataType(sparkDataType: SparkDataType): KernelDataType = {
    sparkDataType match {
      case SparkStringType => KernelStringType.STRING
      case SparkBooleanType => KernelBooleanType.BOOLEAN
      case SparkIntegerType => KernelIntegerType.INTEGER
      case SparkLongType => KernelLongType.LONG
      case x => throw new IllegalArgumentException(s"unsupported data type $x")
    }
  }

}
