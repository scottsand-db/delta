/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class JsonUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonUtils() {}

  /**
   * Parses the given JSON string into a map of key-value pairs.
   *
   * <p>The JSON string should be in the format:
   *
   * <pre>{@code {"key1": "value1", "key2": "value2", ...}}</pre>
   *
   * where both keys and values are strings.
   *
   * @param jsonString The JSON string to parse
   * @return A map containing the key-value pairs extracted from the JSON string
   */
  public static Map<String, String> parseJSONKeyValueMap(String jsonString) {
    if (jsonString == null || jsonString.trim().isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      return MAPPER.readValue(jsonString, new TypeReference<Map<String, String>>() {});
    } catch (Exception e) {
      throw new KernelException(String.format("Failed to parse JSON string: %s", jsonString), e);
    }
  }

  static {
    MAPPER.registerModule(new SimpleModule().addSerializer(Row.class, new RowSerializer()));
  }

  /**
   * Converts a {@link Row} to a single line JSON string. This is currently used just in tests. Wll
   * be used as part of the refactoring planned in <a
   * href="https://github.com/delta-io/delta/issues/2929">#2929</a>
   *
   * @param row the row to convert
   * @return JSON string
   */
  public static String rowToJson(Row row) {
    try {
      return MAPPER.writeValueAsString(row);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException("Could not serialize row object to JSON", ex);
    }
  }

  public static class RowSerializer extends StdSerializer<Row> {
    public RowSerializer() {
      super(Row.class);
    }

    @Override
    public void serialize(Row row, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      writeRow(gen, row, row.getSchema());
    }

    private void writeRow(JsonGenerator gen, Row row, StructType schema) throws IOException {
      gen.writeStartObject();
      for (int columnOrdinal = 0; columnOrdinal < schema.length(); columnOrdinal++) {
        StructField field = schema.at(columnOrdinal);
        if (!row.isNullAt(columnOrdinal)) {
          gen.writeFieldName(field.getName());
          writeValue(gen, row, columnOrdinal, field.getDataType());
        }
      }
      gen.writeEndObject();
    }

    private void writeStruct(JsonGenerator gen, ColumnVector vector, StructType type, int rowId)
        throws IOException {
      gen.writeStartObject();
      for (int columnOrdinal = 0; columnOrdinal < type.length(); columnOrdinal++) {
        StructField field = type.at(columnOrdinal);
        ColumnVector childVector = vector.getChild(columnOrdinal);
        if (!childVector.isNullAt(rowId)) {
          gen.writeFieldName(field.getName());
          writeValue(gen, childVector, rowId, field.getDataType());
        }
      }
      gen.writeEndObject();
    }

    private void writeArrayValue(JsonGenerator gen, ArrayValue arrayValue, ArrayType arrayType)
        throws IOException {
      gen.writeStartArray();
      ColumnVector arrayElems = arrayValue.getElements();
      for (int i = 0; i < arrayValue.getSize(); i++) {
        if (arrayElems.isNullAt(i)) {
          // Jackson serializes the null values in the array, but not in the map
          gen.writeNull();
        } else {
          writeValue(gen, arrayValue.getElements(), i, arrayType.getElementType());
        }
      }
      gen.writeEndArray();
    }

    private void writeMapValue(JsonGenerator gen, MapValue mapValue, MapType mapType)
        throws IOException {
      assertSupportedMapType(mapType);
      gen.writeStartObject();
      ColumnVector keys = mapValue.getKeys();
      ColumnVector values = mapValue.getValues();
      for (int i = 0; i < mapValue.getSize(); i++) {
        gen.writeFieldName(keys.getString(i));
        if (!values.isNullAt(i)) {
          writeValue(gen, values, i, mapType.getValueType());
        } else {
          gen.writeNull();
        }
      }
      gen.writeEndObject();
    }

    private void writeValue(JsonGenerator gen, Row row, int columnOrdinal, DataType type)
        throws IOException {
      checkArgument(!row.isNullAt(columnOrdinal), "value should not be null");
      if (type instanceof BooleanType) {
        gen.writeBoolean(row.getBoolean(columnOrdinal));
      } else if (type instanceof ByteType) {
        gen.writeNumber(row.getByte(columnOrdinal));
      } else if (type instanceof ShortType) {
        gen.writeNumber(row.getShort(columnOrdinal));
      } else if (type instanceof IntegerType) {
        gen.writeNumber(row.getInt(columnOrdinal));
      } else if (type instanceof LongType) {
        gen.writeNumber(row.getLong(columnOrdinal));
      } else if (type instanceof FloatType) {
        gen.writeNumber(row.getFloat(columnOrdinal));
      } else if (type instanceof DoubleType) {
        gen.writeNumber(row.getDouble(columnOrdinal));
      } else if (type instanceof StringType) {
        gen.writeString(row.getString(columnOrdinal));
      } else if (type instanceof StructType) {
        writeRow(gen, row.getStruct(columnOrdinal), (StructType) type);
      } else if (type instanceof ArrayType) {
        writeArrayValue(gen, row.getArray(columnOrdinal), (ArrayType) type);
      } else if (type instanceof MapType) {
        writeMapValue(gen, row.getMap(columnOrdinal), (MapType) type);
      } else {
        // `binary` type is not supported according the Delta Protocol
        throw new UnsupportedOperationException("unsupported data type: " + type);
      }
    }

    private void writeValue(JsonGenerator gen, ColumnVector vector, int rowId, DataType type)
        throws IOException {
      checkArgument(!vector.isNullAt(rowId), "value should not be null");
      if (type instanceof BooleanType) {
        gen.writeBoolean(vector.getBoolean(rowId));
      } else if (type instanceof ByteType) {
        gen.writeNumber(vector.getByte(rowId));
      } else if (type instanceof ShortType) {
        gen.writeNumber(vector.getShort(rowId));
      } else if (type instanceof IntegerType) {
        gen.writeNumber(vector.getInt(rowId));
      } else if (type instanceof LongType) {
        gen.writeNumber(vector.getLong(rowId));
      } else if (type instanceof FloatType) {
        gen.writeNumber(vector.getFloat(rowId));
      } else if (type instanceof DoubleType) {
        gen.writeNumber(vector.getDouble(rowId));
      } else if (type instanceof StringType) {
        gen.writeString(vector.getString(rowId));
      } else if (type instanceof StructType) {
        writeStruct(gen, vector, (StructType) type, rowId);
      } else if (type instanceof ArrayType) {
        writeArrayValue(gen, vector.getArray(rowId), (ArrayType) type);
      } else if (type instanceof MapType) {
        writeMapValue(gen, vector.getMap(rowId), (MapType) type);
      } else {
        throw new UnsupportedOperationException("unsupported data type: " + type);
      }
    }
  }

  private static void assertSupportedMapType(MapType keyType) {
    checkArgument(
        keyType.getKeyType() instanceof StringType,
        "Only STRING type keys are supported in MAP type in JSON serialization");
  }
}
