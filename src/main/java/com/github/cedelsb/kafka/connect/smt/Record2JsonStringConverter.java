/*
 * Copyright © 2021 Christian Edelsbrunner (christian.edelsbrunner@gmail.com)
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

package com.github.cedelsb.kafka.connect.smt;

import com.github.cedelsb.kafka.connect.smt.converter.AvroJsonSchemafulRecordConverter;
import com.github.cedelsb.kafka.connect.smt.converter.JsonSchemalessRecordConverter;
import com.github.cedelsb.kafka.connect.smt.converter.types.json.JsonBinaryConverter;
import com.github.cedelsb.kafka.connect.smt.converter.types.json.JsonDateTimeAsLongConverter;
import com.github.cedelsb.kafka.connect.smt.converter.types.json.JsonDateTimeAsStringConverter;
import com.github.cedelsb.kafka.connect.smt.converter.types.json.JsonDecimalConverter;
import com.github.underscore.U;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.bson.BsonDocument;
import org.bson.json.Converter;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class Record2JsonStringConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static Logger logger = LoggerFactory.getLogger(Record2JsonStringConverter.class);

    public static final String OVERVIEW_DOC =
            "Converts a record value with a schema into a new schema containing a single JSON string field";

    private static final class ConfigName {
        public static final String JSON_STRING_FIELD_NAME = "json.string.field.name";
        public static final String JSON_WRITER_OUTPUT_MODE = "json.writer.output.mode";
        public static final String POST_PROCESSING_TO_XML = "post.processing.to.xml";
        public static final String JSON_WRITER_HANDLE_LOGICAL_TYPES = "json.writer.handle.logical.types";
        public static final String JSON_WRITER_DATETIME_LOGICAL_TYPES_AS = "json.writer.datetime.logical.types.as";
        public static final String JSON_WRITER_DATETIME_PATTERN = "json.writer.datetime.pattern";
        public static final String JSON_WRITER_DATETIME_ZONE_ID = "json.writer.datetime.zoneid";
        public static final String AVRO_UNION_UNWRAP_ENABLED = "avro.union.unwrap.enabled";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.JSON_STRING_FIELD_NAME, ConfigDef.Type.STRING, "jsonstring", ConfigDef.Importance.HIGH,
                    "Field name for output JSON String field")
            .define(ConfigName.JSON_WRITER_OUTPUT_MODE, ConfigDef.Type.STRING, "RELAXED", ConfigDef.Importance.MEDIUM,
                    "Output mode of JSON Writer (RELAXED,EXTENDED,SHELL or STRICT)")
            .define(ConfigName.POST_PROCESSING_TO_XML, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                    "Some old RBDMS like Oracle 11 are not the best in handling JSON - for such scenarios this option can be used to transform the generated JSON into a schemaless XML String")
            .define(ConfigName.JSON_WRITER_HANDLE_LOGICAL_TYPES, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                    "In BSON serialization, logical types (dates, times, timestamps, decimal, bytes) are embedded inside a $<type> field. Setting this configuration to true will remove the embeddings and add the value to the parent field.")
            .define(ConfigName.JSON_WRITER_DATETIME_LOGICAL_TYPES_AS, ConfigDef.Type.STRING, "LONG", ConfigDef.Importance.LOW,
                    "Write the logical type field (of time, date or timestamp) either as a STRING or a LONG (epoc) value, only applicable if json.writer.handle.logical.types=true")
            .define(ConfigName.JSON_WRITER_DATETIME_PATTERN, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                    "The pattern (either a predefined constant or pattern letters) to use to format the date/time or timestamp as string, only applicable if json.writer.datetime.logical.types.as=STRING")
            .define(ConfigName.JSON_WRITER_DATETIME_ZONE_ID, ConfigDef.Type.STRING, "UTC", ConfigDef.Importance.LOW,
                    "The zone id to use to format the date/time or timestamp as string, only applicable if json.writer.datetime.logical.types.as=STRING")
            .define(ConfigName.AVRO_UNION_UNWRAP_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                    "Enable unwrapping of Avro union types. When enabled, union values are unwrapped to their actual value instead of outputting all union branches. Only the selected branch value is output, other branches (which are null) are omitted.");

    private static final String PURPOSE = "Converting record with Schema into a simple JSON String";

    private String jsonStringFieldName;
    private Schema jsonStringOutputSchema;
    private boolean transformToXML;
    private String writeDatetimeLogicalTypesAs;
    private String writeDatetimeWithPattern;
    private String writeDatetimeWithZoneId;

    private boolean handleLogicalTypes;
    private boolean avroUnionUnwrapEnabled;

    AvroJsonSchemafulRecordConverter converter;
    JsonSchemalessRecordConverter converterWithoutSchema;
    JsonWriterSettings jsonWriterSettings;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        jsonStringFieldName = config.getString(ConfigName.JSON_STRING_FIELD_NAME);
        jsonStringOutputSchema = makeJsonStringOutputSchema();
        handleLogicalTypes = config.getBoolean(ConfigName.JSON_WRITER_HANDLE_LOGICAL_TYPES);
        writeDatetimeLogicalTypesAs = config.getString(ConfigName.JSON_WRITER_DATETIME_LOGICAL_TYPES_AS);
        writeDatetimeWithPattern = config.getString(ConfigName.JSON_WRITER_DATETIME_PATTERN);
        writeDatetimeWithZoneId = config.getString(ConfigName.JSON_WRITER_DATETIME_ZONE_ID);
        avroUnionUnwrapEnabled = config.getBoolean(ConfigName.AVRO_UNION_UNWRAP_ENABLED);

        if (handleLogicalTypes) {
            Converter<Long> dateTimeConverter = null;
            if (writeDatetimeLogicalTypesAs.equalsIgnoreCase("STRING")) {
                dateTimeConverter = new JsonDateTimeAsStringConverter(writeDatetimeWithPattern, writeDatetimeWithZoneId);
            } else if (writeDatetimeLogicalTypesAs.equalsIgnoreCase("LONG")) {
                dateTimeConverter = new JsonDateTimeAsLongConverter();
            } else {
                throw new IllegalArgumentException("Wrong value for configuration setting: " + ConfigName.JSON_WRITER_DATETIME_LOGICAL_TYPES_AS + "=" + writeDatetimeLogicalTypesAs);
            }

            jsonWriterSettings = JsonWriterSettings
                    .builder()
                    .outputMode(toJsonMode(config.getString(ConfigName.JSON_WRITER_OUTPUT_MODE)))
                    .dateTimeConverter(dateTimeConverter)
                    .decimal128Converter(new JsonDecimalConverter())
                    .binaryConverter(new JsonBinaryConverter())
                    .build();
        } else {
            jsonWriterSettings = JsonWriterSettings
                    .builder()
                    .outputMode(toJsonMode(config.getString(ConfigName.JSON_WRITER_OUTPUT_MODE)))
                    .build();
        }

        converter = new AvroJsonSchemafulRecordConverter(avroUnionUnwrapEnabled);
        converterWithoutSchema = new JsonSchemalessRecordConverter();

        transformToXML = config.getBoolean(ConfigName.POST_PROCESSING_TO_XML);
    }

    @Override
    public R apply(R record) {

        if (isTombstoneRecord(record))
            return record;

        Schema schema = operatingSchema(record);
        Object value = operatingValue(record);

        BsonDocument bsonDoc;
        if (schema != null) {
            bsonDoc = convertWithSchema(schema, value);
        } else {
            bsonDoc = convertWithoutSchema(value);
        }

        final Struct jsonStringOutputStruct = new Struct(jsonStringOutputSchema);
        String outputDocument = bsonDoc.toJson(jsonWriterSettings);

        if (transformToXML) {
            outputDocument = U.jsonToXml(outputDocument);
        }

        jsonStringOutputStruct.put(jsonStringFieldName, outputDocument);

        return newRecord(record, jsonStringOutputSchema, jsonStringOutputStruct);
    }

    private BsonDocument convertWithSchema(Schema schema, Object value) {
        final Struct struct = requireStruct(value, PURPOSE);
        return converter.convert(schema, struct);
    }

    private BsonDocument convertWithoutSchema(Object value) {
        return converterWithoutSchema.convert(makeDefaultSchema(), value);
    }

    private Schema makeDefaultSchema() {
        return SchemaBuilder
                .struct()
                .name("defaultSchema")
                .version(1)
                .field("value", Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }


    @Override
    public void close() {
        converter = null;
    }

    private Schema makeJsonStringOutputSchema() {
        return SchemaBuilder
                .struct()
                .name("jsonStringSchema")
                .version(1)
                .field(jsonStringFieldName, Schema.STRING_SCHEMA)
                .build();
    }

    private JsonMode toJsonMode(String jsonMode) {
        switch (jsonMode) {
            case "SHELL":
                return JsonMode.SHELL;
            case "EXTENDED":
                return JsonMode.EXTENDED;
            case "STRICT":
                return JsonMode.STRICT;
            default:
                return JsonMode.RELAXED;
        }
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    protected abstract boolean isTombstoneRecord(R record);

    public static class Key<R extends ConnectRecord<R>> extends Record2JsonStringConverter<R> {

        @Override
        protected Schema operatingSchema(R record) {

            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {

            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

        @Override
        protected boolean isTombstoneRecord(R record) {
            return record.key() == null;
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends Record2JsonStringConverter<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

        @Override
        protected boolean isTombstoneRecord(R record) {
            return record.value() == null;
        }

    }
}
