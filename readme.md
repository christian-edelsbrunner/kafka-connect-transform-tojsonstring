[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=an0r0c_kafka-connect-transform-tojsonstring&metric=alert_status)](https://sonarcloud.io/dashboard?id=an0r0c_kafka-connect-transform-tojsonstring)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=an0r0c_kafka-connect-transform-tojsonstring&metric=coverage)](https://sonarcloud.io/dashboard?id=an0r0c_kafka-connect-transform-tojsonstring)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=an0r0c_kafka-connect-transform-tojsonstring&metric=security_rating)](https://sonarcloud.io/dashboard?id=an0r0c_kafka-connect-transform-tojsonstring)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=an0r0c_kafka-connect-transform-tojsonstring&metric=vulnerabilities)](https://sonarcloud.io/dashboard?id=an0r0c_kafka-connect-transform-tojsonstring)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=an0r0c_kafka-connect-transform-tojsonstring&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=an0r0c_kafka-connect-transform-tojsonstring)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=an0r0c_kafka-connect-transform-tojsonstring&metric=bugs)](https://sonarcloud.io/dashboard?id=an0r0c_kafka-connect-transform-tojsonstring)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=an0r0c_kafka-connect-transform-tojsonstring&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=an0r0c_kafka-connect-transform-tojsonstring)

# kafka-connect-transform-tojsonstring - A simple Record to JSON String SMT
This is a very simple Kafka Connect SMT which takes the entire key or value record and transforms it to a new record which contains exactly one field with a JSON representation of the origin record. 

Blog Post describing how we ended up developing this SMT can be found [here](https://medium.com/bearingpoint-technology-advisory/handle-arrays-and-nested-arrays-in-kafka-jdbc-sink-connector-41929ea46301?source=friends_link&sk=b7028711b4945c820f647df950cdd949) 

## Use Cases
The reason why this SMT was built is the known limitation of the JDBC Sink Connector tohandle nested arrays. If you have schema which contains arrays you cannot really use the JDBC Sink Connector because this connector only supports primitive Data Types. 
But sometimes you just need also some arrays from the schema in the RDBMS. If your RDBMS is able to handle JSON Strings this SMT might be the saviour. You can use it to transform the whole record into a single JSON String which can be mapped by the JDBC Sink connector. 
Afterwards you can use the tools offered by the RDBMS to parse and process the JSON String.

But for sure there are also other use cases out there where this SMT might be helpful.

## Restrictions
This SMT was built to transform Records **with a Schema** to a new Record with a Schema but with only one field. 
So this SMT does not work for Schemaless records. 

It also was only tested with Avro Schemas backed by Confluent Schema Registry (but most likely will work for other schema variants too because the deserializer already converted the record to a Connect Record before so it shouldn't be schema specific)

## Configuration
```json5
{
  ////
  "transforms": "tojson",
  "transforms.tojson.type": "com.github.cedelsb.kafka.connect.smt.Record2JsonStringConverter$Value",
  "transforms.tojson.json.string.field.name" : "myawesomejsonstring", // Optional
  "transforms.tojson.post.processing.to.xml" : false, // Optional
  "transforms.tojson.json.writer.handle.logical.types" : true, // Optional
  "transforms.tojson.json.writer.datetime.logical.types.as" : "STRING", // Optional
  "transforms.tojson.json.writer.datetime.pattern" : "", // Optional
  "transforms.tojson.json.writer.datetime.zoneid" : "UTC", // Optional
  "transforms.tojson.avro.union.unwrap.enabled" : false // Optional
  ////
}
```

### Parameters

<table class="data-table"><tbody>
<tr><th>Name</th><th>Description</th><th>Type</th><th>Default</th><th>Valid Values</th><th>Importance</th></tr>
<tr>
<td>json.string.field.name</td>
<td>Output schema field name of field that contains the JSON String</td><td>string</td><td></td><td>non-empty string</td><td>high</td>
</tr>
<tr>
<td>json.writer.output.mode</td>
<td>Output mode of the BSON Json Writer</td><td>string</td><td>RELAXED</td><td>RELAXED, EXTENDED, STRICT or SHELL</td><td>high</td>
</tr>
<tr>
<td>post.processing.to.xml</td>
<td>Post Process JSON to XML. Some old RBDMS like Oracle 11 are not the best in handling JSON - for such scenarios this option can be used to transform the generated JSON into a schemaless XML String</td>
<td>boolean</td><td>false</td><td>true/false</td><td>high</td>
</tr>
<tr>
<td>json.writer.handle.logical.types</td>
<td>In BSON serialization, logical types (dates, times, timestamps, decimal, bytes) are embedded inside a $<type> field. Setting this configuration to true will remove the embeddings and add the value to the parent field.</td>
<td>boolean</td><td>false</td><td>true/false</td><td>high</td>
</tr>
<tr>
<td>json.writer.datetime.logical.types.as</td>
<td>Write the logical type field (of time, date or timestamp) either as a STRING or a LONG (epoc) value, only applicable if json.writer.handle.logical.types=true</td>
<td>string</td><td>LONG</td><td>LONG/STRING</td><td>high</td>
</tr>
<tr>
<td>json.writer.datetime.pattern</td>
<td>The pattern (either a predefined constant or pattern letters) to use to format the date/time or timestamp as string, only applicable if json.writer.datetime.logical.types.as=STRING</td>
<td>string</td>ISO_INSTANT<td></td><td>ISO_DATE,ISO_DATE_TIME,ISO_INSTANT,ISO_TIME,ISO_LOCAL_DATE,ISO_LOCAL_DATE_TIME,ISO_LOCAL_TIME,RFC_1123_DATE_TIME,ISO_ZONED_DATE_TIME,ISO_OFFSET_DATE,ISO_OFFSET_DATE_TIME,ISO_OFFSET_TIME,BASIC_ISO_DATE,ISO_ORDINAL_DATE,ISO_WEEK_DATE,"pattern"</td><td>high</td>
</tr>
<tr>
<td>json.writer.datetime.zoneid</td>
<td>The ZoneId to use to format the date/time or timestamp as string, only applicable if json.writer.datetime.logical.types.as=STRING</td>
<td>string</td><td>UTC</td><td>a valid ZoneId string, such as Europe/Zurich, CET or UTC</td><td>high</td>
</tr>
<tr>
<td>avro.union.unwrap.enabled</td>
<td>Enable unwrapping of Avro union types. When enabled, union values are unwrapped to their actual value instead of outputting all union branches. When Avro unions are converted to Kafka Connect Structs (Confluent format), the struct contains fields for all union branches with the selected branch holding its value and other branches as null. With this setting enabled, only the actual selected value is output, omitting null branches.</td>
<td>boolean</td><td>false</td><td>true/false</td><td>medium</td>
</tr>
</tbody></table>

## Example

##### Input 

* Schema (avro syntax)

```json5
{
	"type": "record",
	"name": "MyEntity",
	"fields": [{
		"name": "id",
		"type": "string"
	},
	{
		"name": "name",
		"type": "string"
	},
	{
		"name": "subElements",
		"type": {
			"type": "array",
			"items": {
				"type": "record",
				"name": "element",
				"fields": [{
					"name": "id",
					"type": "string",
					
				}]
			}
		}
	}]
}
```

* Value 

```
-id:myobject
-name:awesomename
-subElements:
  -id:element1
  -id:element2 
```
##### Output 
* Schema
```json5
{
	"type": "record",
	"name": "jsonStringSchema",
	"fields": [{
		"name": "jsonstring",
		"type": "string"
	}]
}
```
* Value (of the schema field "jsonstring")
```json5
{
	"id": "record",
	"name": "jsonStringSchema",
	"subElements": [{"id": "element1"},
                    {"id": "element2"}]
	}]
} 
```
## Build, installation / deployment
You can build this project from sources via Maven. 

Or download a pre-build release from [Releases](https://github.com/an0r0c/kafka-connect-transform-tojsonstring/releases) 

## Thanks and Acknowledgement
Basic structure of how to build a basic SMT was taken from [kafka-connect-insert-uuid](https://github.com/cjmatta/kafka-connect-insert-uuid)

Logic for transforming a Connect Record into a Json Document is build up on the awesome converter implemented in [kafka-connect-mongodb](https://github.com/hpgrahsl/kafka-connect-mongodb) which safed me a lot of time and nerves :)  

## License Information

This project is licensed according to [Apache License Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

```
Copyright (c) 2021. Christian Edelsbrunner (christian.edelsbrunner@gmail.com) 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
