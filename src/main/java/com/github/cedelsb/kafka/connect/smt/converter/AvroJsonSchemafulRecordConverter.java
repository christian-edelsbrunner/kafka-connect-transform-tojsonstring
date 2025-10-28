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

        if (isSupportedLogicalType(field.schema())) {
            doc.put(field.name(), getConverter(field.schema()).toBson(struct.get(field), field.schema()));
            return;
        }

        try {
            switch (field.schema().type()) {
                case BOOLEAN:
                case FLOAT32:
                case FLOAT64:
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case STRING:
                case BYTES:
                    handlePrimitiveField(doc, struct.get(field), field);
                    break;
                case STRUCT:
                    handleStructField(doc, (Struct) struct.get(field), field);
                    break;
                case ARRAY:
                    doc.put(field.name(), handleArrayField((List) struct.get(field), field));
                    break;
                case MAP:
                    handleMapField(doc, (Map) struct.get(field), field);
                    break;
                default:
                    throw new DataException("unexpected / unsupported schema type " + field.schema().type());
            }
        } catch (Exception exc) {
            logger.error("Error processing field '{}' of type '{}': {}",
                         field.name(), field.schema().type(), exc.getMessage());
            throw new DataException("error while processing field " + field.name(), exc);
        }
    }

    private void handleMapField(BsonDocument doc, Map m, Field field) {
        logger.trace("handling map field='{}' valueSchema.type='{}'",
                     field.name(), field.schema().valueSchema().type());

        if (m == null) {
            logger.trace("  field='{}' has null map", field.name());
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }

        BsonDocument mapDoc = convertMapValue(field.schema(), (Map<String, Object>) m);
        doc.put(field.name(), mapDoc);
    }

    private BsonValue handleArrayField(List list, Field field) {
        logger.trace("handling array field='{}' valueSchema.type='{}'",
                     field.name(), field.schema().valueSchema().type());

        if (list == null) {
            logger.trace("  array is null");
            return BsonNull.VALUE;
        }

        BsonArray array = new BsonArray();
        Schema valueSchema = field.schema().valueSchema();

        for (Object element : list) {
            BsonValue convertedElement = convertValue(valueSchema, element);
            array.add(convertedElement);
        }

        return array;
    }

    private void handleStructField(BsonDocument doc, Struct struct, Field field) {
        logger.trace("handling struct field='{}' schema.name='{}' schema.type='{}'",
                     field.name(), field.schema().name(), field.schema().type());

        if (struct == null) {
            logger.trace("  field='{}' has null struct value", field.name());
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }

        logger.trace("  struct value: {}", struct.toString());

        boolean isUnion = unionUnwrapEnabled && isUnionStruct(field.schema());
        logger.trace("  isUnionStruct={} unionUnwrapEnabled={} for schema.name='{}'",
                     isUnion, unionUnwrapEnabled, field.schema().name());

        if (isUnion) {
            unwrapAndPutUnion(doc, struct, field);
        } else {
            logger.trace("  processing as regular struct with {} fields", field.schema().fields().size());
            doc.put(field.name(), toBsonDoc(field.schema(), struct));
        }
    }

    /**
     * Unwraps an Avro union struct and outputs only the selected branch value.
     * Union structs have multiple optional fields (one per branch), but only one should be non-null.
     */
    private void unwrapAndPutUnion(BsonDocument doc, Struct struct, Field field) {
        logger.trace("unwrapping union field='{}' schema.name='{}'",
                     field.name(), field.schema().name());

        int nonNullBranches = 0;

        for (Field unionBranch : field.schema().fields()) {
            Object branchValue = struct.get(unionBranch);
            logger.trace("  union branch='{}' type='{}' value.isNull={}",
                         unionBranch.name(), unionBranch.schema().type(), branchValue == null);

            if (branchValue != null) {
                nonNullBranches++;
                logger.trace("  selected branch='{}' type='{}' - unwrapping to parent field",
                             unionBranch.name(), unionBranch.schema().type());

                BsonValue unwrappedValue = convertValue(unionBranch.schema(), branchValue);
                doc.put(field.name(), unwrappedValue);
                break; // Only one branch should have a value
            }
        }

        logger.trace("  union unwrapping complete for field='{}', nonNullBranches={}",
                     field.name(), nonNullBranches);

        // If all branches were null, output BsonNull
        if (!doc.containsKey(field.name())) {
            logger.trace("  all union branches null for field='{}' - adding BsonNull", field.name());
            doc.put(field.name(), BsonNull.VALUE);
        }
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
            return getConverter(schema).toBson(value, schema);
        }

        switch (type) {
            case STRUCT:
                return convertStructValue(schema, (Struct) value);
            case ARRAY:
                Field arrayField = new Field("temp", 0, schema);
                return handleArrayField((List) value, arrayField);
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

        if (unionUnwrapEnabled && isUnionStruct(schema)) {
            logger.trace("convertStructValue: detected union struct, unwrapping");
            BsonDocument tempDoc = new BsonDocument();
            Field tempField = new Field("temp", 0, schema);
            unwrapAndPutUnion(tempDoc, struct, tempField);
            return tempDoc.get("temp");
        } else {
            return toBsonDoc(schema, struct);
        }
    }

    /**
     * Converts a map to a BsonDocument, handling union-typed values.
     */
    private BsonDocument convertMapValue(Schema mapSchema, Map<String, Object> mapValue) {
        logger.trace("convertMapValue: processing map with {} entries", mapValue.size());

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

    private void handlePrimitiveField(BsonDocument doc, Object value, Field field) {
        logger.trace("handling primitive type '{}' name='{}'", field.schema().type(), field.name());
        doc.put(field.name(), getConverter(field.schema()).toBson(value, field.schema()));
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
