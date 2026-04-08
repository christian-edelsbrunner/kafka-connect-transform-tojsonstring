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

package com.github.cedelsb.integration;

import com.esotericsoftware.yamlbeans.YamlReader;
import okhttp3.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class Record2JsonStringIT {

    public static final String DOCKER_COMPOSE_FILE = "src/test/resources/docker/compose.yaml";
    public static final String SINK_CONNECTOR_CONFIG = "src/test/resources/docker/jdbcsinkconnector.json";
    public static final String DEFAULT_COMPOSE_SERVICE_SUFFIX = "_1";


    public static final String KAFKA_BROKER;
    public static final int KAFKA_BROKER_PORT;

    public static final String KAFKA_CONNECT;
    public static final int KAFKA_CONNECT_PORT;

    public static final String SCHEMA_REGISTRY;
    public static final int SCHEMA_REGISTRY_PORT;

    public static final String POSTGRES;
    public static int POSTGRES_PORT;
    static {
        try {
            Map composeFile = (Map)new YamlReader(new FileReader(DOCKER_COMPOSE_FILE)).read();

            KAFKA_BROKER = extractHostnameFromDockerCompose(composeFile,"kafkabroker");
            KAFKA_BROKER_PORT = extractHostPortFromDockerCompose(composeFile,"kafkabroker");

            KAFKA_CONNECT = extractHostnameFromDockerCompose(composeFile,"kafkaconnect");
            KAFKA_CONNECT_PORT = extractHostPortFromDockerCompose(composeFile,"kafkaconnect");

            SCHEMA_REGISTRY = extractHostnameFromDockerCompose(composeFile,"schemaregistry");
            SCHEMA_REGISTRY_PORT = extractHostPortFromDockerCompose(composeFile,"schemaregistry");

            POSTGRES = extractHostnameFromDockerCompose(composeFile,"postgres");
            POSTGRES_PORT = extractHostPortFromDockerCompose(composeFile,"postgres");
        } catch(Exception exc) {
            throw new RuntimeException("error: parsing the docker-compose YAML",exc);
        }
    }

    public static final DockerComposeContainer CONTAINER_ENV =
            new DockerComposeContainer(new File(DOCKER_COMPOSE_FILE))
                    .withOptions("--compatibility")
                    .withLocalCompose(true)
                    .withExposedService(KAFKA_BROKER+DEFAULT_COMPOSE_SERVICE_SUFFIX,KAFKA_BROKER_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                    .withExposedService(SCHEMA_REGISTRY +DEFAULT_COMPOSE_SERVICE_SUFFIX, SCHEMA_REGISTRY_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                    .withExposedService(KAFKA_CONNECT+DEFAULT_COMPOSE_SERVICE_SUFFIX,KAFKA_CONNECT_PORT,
                            Wait.forLogMessage(".*Kafka Connect started.*\\n", 1)
                                    .withStartupTimeout(Duration.ofSeconds(600)))
                    .withExposedService(POSTGRES+DEFAULT_COMPOSE_SERVICE_SUFFIX,POSTGRES_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))

            ;

    @BeforeAll
    public static void setup() throws IOException, InterruptedException {
        CONTAINER_ENV.start();

        String config = new String(Files.readAllBytes(Paths.get(SINK_CONNECTOR_CONFIG)));

        // Wait another 5 Sec to be sure connect is up and ready
        Thread.sleep(5000);

        registerJDBCSinkConnector(config);

    }

    @AfterAll
    public static void teardown() {
        if (CONTAINER_ENV != null) {
            CONTAINER_ENV.stop();
        }
    }

    @Test
    public void sendRecordAndTransformItToJSONString() throws ExecutionException, InterruptedException {

        // Build Test Avro Message
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.github.cedelsb.integration\",\n" +
                "     \"name\": \"TestRecord\",\n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"field1\", \"type\": \"string\"  },\n" +
                "       { \"name\": \"field2\", \"type\": \"string\" },\n" +
                "       { \"name\": \"field3\", \"type\": \"int\"},\n" +
                "       { \"name\": \"field4\", \"type\": \"float\" },\n" +
                "       { \"name\": \"field5\", \"type\": \"float\" },\n" +
                "       { \"name\": \"field6\", \"type\": \"boolean\", \"default\": true}\n" +
                "     ]\n" +
                "}");

        GenericRecordBuilder avroBuilder = new GenericRecordBuilder(schema);

        avroBuilder.set(schema.getField("field1"), "TestValue");
        avroBuilder.set(schema.getField("field2"), "TestValue2");
        avroBuilder.set(schema.getField("field3"), 1337);
        avroBuilder.set(schema.getField("field4"), 1.5f);
        avroBuilder.set(schema.getField("field5"), 3.14f);

        avroBuilder.build();

        // Send it to Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:" + KAFKA_BROKER_PORT);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url","http://"+SCHEMA_REGISTRY+":"+ SCHEMA_REGISTRY_PORT);

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("smttest", avroBuilder.build());

        producer.send(record, (RecordMetadata r, Exception exc) -> {
            assertNull(exc, () -> "unexpected error while sending"
                    + " | exc: " + exc.getMessage()
            );
        }).get();

        //Give the connector time to do his work
        Thread.sleep(1000);

        java.sql.Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres?currentSchema=smttest", "postgres", "postgres");

            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM smttest.smttest");

            assertTrue(rs.next());

            assertEquals("{\"field1\": \"TestValue\", \"field2\": \"TestValue2\", \"field3\": 1337, \"field4\": 1.5, \"field5\": 3.140000104904175, \"field6\": true}",rs.getString(1));

            rs.close();
            st.close();

            conn.close();
        } catch (SQLException e) {
            assertNull(e, () -> "unexpected error while sending"
                    + " | exc: " + e.getMessage()
            );
        }

    }

    private static void registerJDBCSinkConnector(String configuration) throws IOException {
        RequestBody body = RequestBody.create(
                MediaType.parse("application/json"), configuration
        );

        Request request = new Request.Builder()
                .url("http://localhost:"+KAFKA_CONNECT_PORT+"/connectors")
                .post(body)
                .build();

        Response response = new OkHttpClient().newCall(request).execute();
        assert(response.code() == 201);
        response.close();
    }

    private static void deferExecutionToWaitForDataPropagation(Duration delay, String message) {
        System.out.println(message);
        try {
            System.out.println("sleeping for "+delay.toMillis()+" millis");
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {}
    }

    private static String extractHostnameFromDockerCompose(Map compose,String serviceName) {
        return (String)((Map)((Map)compose.get("services")).get(serviceName)).get("hostname");
    }

    private static int extractHostPortFromDockerCompose(Map compose,String serviceName) {
        return Integer.parseInt(((String)((List)((Map)((Map)compose.get("services"))
                .get(serviceName)).get("ports")).get(0)).split(":")[1]);
    }

}
