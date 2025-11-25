/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.cedelsb.kafka.connect.smt.converter;

import com.github.cedelsb.kafka.connect.smt.converter.types.sink.bson.*;
import com.github.cedelsb.kafka.connect.smt.converter.types.sink.bson.logical.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Converts Kafka Connect records with Avro or JSON schemas to BSON documents.
 *
 * Looks like Avro and JSON + Schema are convertible by means of a unified
 * conversion approach since they are using the same the Struct/Type information.
 */
public class AvroJsonSchemafulRecordConverter implements RecordConverter {

    private static final Logger logger = LoggerFactory.getLogger(AvroJsonSchemafulRecordConverter.class);
    private static final String CONFLUENT_UNION_MARKER = "io.confluent.connect.avro.Union";

    public static final Set<String> LOGICAL_TYPE_NAMES = new HashSet<>(
            Arrays.asList(Date.LOGICAL_NAME, Decimal.LOGICAL_NAME,
                          Time.LOGICAL_NAME, Timestamp.LOGICAL_NAME)
    );

    private final Map<Schema.Type, SinkFieldConverter> converters = new HashMap<>();
    private final Map<String, SinkFieldConverter> logicalConverters = new HashMap<>();
    private final boolean unionUnwrapEnabled;

    public AvroJsonSchemafulRecordConverter() {
        this(false);
    }

    public AvroJsonSchemafulRecordConverter(boolean unionUnwrapEnabled) {
        this.unionUnwrapEnabled = unionUnwrapEnabled;
        registerStandardConverters();
        registerLogicalConverters();
    }

    private void registerStandardConverters() {
        registerSinkFieldConverter(new BooleanFieldConverter());
        registerSinkFieldConverter(new Int8FieldConverter());
        registerSinkFieldConverter(new Int16FieldConverter());
        registerSinkFieldConverter(new Int32FieldConverter());
        registerSinkFieldConverter(new Int64FieldConverter());
        registerSinkFieldConverter(new Float32FieldConverter());
        registerSinkFieldConverter(new Float64FieldConverter());
        registerSinkFieldConverter(new StringFieldConverter());
        registerSinkFieldConverter(new BytesFieldConverter());
    }

    private void registerLogicalConverters() {
        registerSinkFieldLogicalConverter(new DateFieldConverter());
        registerSinkFieldLogicalConverter(new TimeFieldConverter());
        registerSinkFieldLogicalConverter(new TimestampFieldConverter());
        registerSinkFieldLogicalConverter(new DecimalFieldConverter());
    }

    @Override
    public BsonDocument convert(Schema schema, Object value) {
        if (schema == null || value == null) {
            throw new DataException("error: schema and/or value was null for AVRO conversion");
        }

        logger.trace("convert() entry: schema.name='{}' schema.type='{}' value.class='{}'",
                     schema.name(), schema.type(), value.getClass().getSimpleName());

        return toBsonDoc(schema, value);
    }

    private void registerSinkFieldConverter(SinkFieldConverter converter) {
        converters.put(converter.getSchema().type(), converter);
    }

    private void registerSinkFieldLogicalConverter(SinkFieldConverter converter) {
        logicalConverters.put(converter.getSchema().name(), converter);
    }

    private BsonDocument toBsonDoc(Schema schema, Object value) {
        BsonDocument doc = new BsonDocument();
        schema.fields().forEach(f -> processField(doc, (Struct) value, f));
        return doc;
    }

    /**
     * Detects if a schema represents an Avro union type using Confluent's marker.
     */
    private boolean isUnionStruct(Schema schema) {
        return schema != null
               && schema.type() == Schema.Type.STRUCT
               && CONFLUENT_UNION_MARKER.equals(schema.name());
    }

    private void processField(BsonDocument doc, Struct struct, Field field) {
        logger.trace("processing field '{}'", field.name());

        try {
            BsonValue value = convertValue(field.schema(), struct.get(field));
            doc.put(field.name(), value);
        } catch (Exception exc) {
            logger.error("Error processing field '{}' of type '{}': {}",
                         field.name(), field.schema().type(), exc.getMessage());
            throw new DataException("error while processing field " + field.name(), exc);
        }
    }

    private BsonValue convertSimpleValue(Schema schema, Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        }

        if (isSupportedLogicalType(schema)) {
            logger.trace("converting logical type '{}'", schema.name());
        } else {
            logger.trace("converting primitive type '{}'", schema.type());
        }

        return getConverter(schema).toBson(value, schema);
    }

    /**
     * Unwraps an Avro union struct and returns the selected branch value.
     * Union structs have multiple optional fields (one per branch), but only one should be non-null.
     */
    private BsonValue unwrapUnion(Schema schema, Struct struct) {
        logger.trace("unwrapping union schema.name='{}'", schema.name());

        for (Field unionBranch : schema.fields()) {
            Object branchValue = struct.get(unionBranch);
            logger.trace("  union branch='{}' type='{}' value.isNull={}",
                         unionBranch.name(), unionBranch.schema().type(), branchValue == null);

            if (branchValue != null) {
                logger.trace("  selected branch='{}' type='{}' - unwrapping to parent field",
                             unionBranch.name(), unionBranch.schema().type());
                return convertValue(unionBranch.schema(), branchValue);
            }
        }

        logger.trace("  all union branches null - returning BsonNull");
        return BsonNull.VALUE;
    }

    /**
     * Converts a value based on its schema type. Handles primitives, structs, arrays, and maps.
     * This is a central conversion point that eliminates duplication.
     */
    private BsonValue convertValue(Schema schema, Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        }

        Schema.Type type = schema.type();

        if (type.isPrimitive() || isSupportedLogicalType(schema)) {
            return convertSimpleValue(schema, value);
        }

        switch (type) {
            case STRUCT:
                return convertStructValue(schema, (Struct) value);
            case ARRAY:
                return convertArrayValue(schema, (List) value);
            case MAP:
                return convertMapValue(schema, (Map<String, Object>) value);
            default:
                throw new DataException("Unsupported schema type: " + type);
        }
    }

    /**
     * Converts a struct value, detecting and unwrapping unions if enabled and necessary.
     */
    private BsonValue convertStructValue(Schema schema, Struct struct) {
        if (struct == null) {
            logger.trace("  struct is null");
            return BsonNull.VALUE;
        }

        logger.trace("converting struct schema.name='{}' schema.type='{}' value: {}",
                     schema.name(), schema.type(), struct.toString());

        boolean isUnion = isUnionStruct(schema);
        logger.trace("  isUnionStruct={} unionUnwrapEnabled={} for schema.name='{}'",
                     isUnion, unionUnwrapEnabled, schema.name());

        if (unionUnwrapEnabled && isUnion) {
            return unwrapUnion(schema, struct);
        } else {
            logger.trace("  processing as regular struct with {} fields", schema.fields().size());
            return toBsonDoc(schema, struct);
        }
    }

    private BsonValue convertArrayValue(Schema arraySchema, List arrayValue) {
        if (arrayValue == null) {
            logger.trace("  array is null");
            return BsonNull.VALUE;
        }

        logger.trace("converting array valueSchema.type='{}'", arraySchema.valueSchema().type());

        BsonArray array = new BsonArray();
        Schema valueSchema = arraySchema.valueSchema();

        for (Object element : arrayValue) {
            BsonValue convertedElement = convertValue(valueSchema, element);
            array.add(convertedElement);
        }

        return array;
    }

    /**
     * Converts a map to a BsonValue, handling union-typed values and null maps.
     */
    private BsonValue convertMapValue(Schema mapSchema, Map<String, Object> mapValue) {
        if (mapValue == null) {
            logger.trace("  map is null");
            return BsonNull.VALUE;
        }

        logger.trace("converting map valueSchema.type='{}' entries={}",
                     mapSchema.valueSchema().type(), mapValue.size());

        BsonDocument mapDoc = new BsonDocument();
        Schema valueSchema = mapSchema.valueSchema();

        for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            logger.trace("  map entry key='{}' valueSchemaType='{}' value.isNull={}",
                         key, valueSchema.type(), value == null);

            BsonValue convertedValue = convertValue(valueSchema, value);
            mapDoc.put(key, convertedValue);
        }

        return mapDoc;
    }

    private boolean isSupportedLogicalType(Schema schema) {
        return schema.name() != null && LOGICAL_TYPE_NAMES.contains(schema.name());
    }

    private SinkFieldConverter getConverter(Schema schema) {
        SinkFieldConverter converter;

        if (isSupportedLogicalType(schema)) {
            converter = logicalConverters.get(schema.name());
        } else {
            converter = converters.get(schema.type());
        }

        if (converter == null) {
            throw new ConnectException("error no registered converter found for " + schema.type().getName());
        }

        return converter;
    }
}
