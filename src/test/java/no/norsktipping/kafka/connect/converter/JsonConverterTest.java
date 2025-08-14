package no.norsktipping.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.norsktipping.kafka.connect.converter.JsonConverter.createLogicalTypesStringSchema;

public class JsonConverterTest {
    private static final String TOPIC = "topic";
    private static KafkaAvroSerializer serializer;
    private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081");

    private final SchemaRegistryClient schemaRegistry;
    private final JsonConverter converter;

    public JsonConverterTest() {
        schemaRegistry = new MockSchemaRegistryClient();
        converter = new JsonConverter(schemaRegistry);
    }

    @Before
    public void setUp() {
          }

    @Test
    public void testCustomerStateTopic() throws RestClientException, IOException {
        Map<String, String> map = Stream.of(
                        new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                        new AbstractMap.SimpleImmutableEntry<>("schema.names", "customer"),
                        new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
                        new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
                        new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
                        new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
                        new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INCLUDENAMESPACE, "true")
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        converter.configure(map, false);

        org.apache.avro.Schema nameSchema = org.apache.avro.SchemaBuilder.builder()
                .record("Name")
                .fields()
                .requiredString("firstname")
                .requiredString("lastname")
                .endRecord();


        org.apache.avro.Schema addressSchema = org.apache.avro.SchemaBuilder
                .record("Address")
                .fields()
                .optionalString("type")
                .optionalString("address1")
                .endRecord();

        org.apache.avro.Schema addressListSchema = org.apache.avro.SchemaBuilder
                .array().items(addressSchema);

        org.apache.avro.Schema stateSchema = org.apache.avro.SchemaBuilder
                .record("state")
                .namespace("nt.customer.internal")
                .fields()
                    .requiredString("customerId")
                    .name("Name").type(nameSchema).noDefault()
                    .name("Address").type(addressListSchema).noDefault()
                .endRecord();

        org.apache.avro.Schema addressRemovedSchema = org.apache.avro.SchemaBuilder
                .record("AddressRemoved")
                .fields()
                .name("Address").type(addressSchema).noDefault()
                .endRecord();

        org.apache.avro.Schema addressAddedSchema = org.apache.avro.SchemaBuilder
                .record("AddressAdded")
                .fields()
                .name("Address").type(addressSchema).noDefault()
                .endRecord();

        org.apache.avro.Schema eventSchema = org.apache.avro.SchemaBuilder
                .unionOf().type(addressAddedSchema)
                .and().type(addressRemovedSchema)
                .endUnion();

        org.apache.avro.Schema customerSchema = org.apache.avro.SchemaBuilder
                .record("customer")
                .namespace("nt.customer.internal")
                .fields()
                .name("CustomerState").type(stateSchema).noDefault()
                .name("Event").type(eventSchema).noDefault()
                .endRecord();


        System.out.println(customerSchema);

        GenericData.Record struct = new GenericRecordBuilder(customerSchema)
                .set("CustomerState", new GenericRecordBuilder(stateSchema)
                    .set("customerId", "id01")
                    .set("Name", new GenericRecordBuilder(nameSchema)
                        .set("firstname", "Jørn")
                        .set("lastname", "Hanserud")
                        .build())
                    .set("Address", Arrays.asList(
                            new GenericRecordBuilder(addressSchema)
                            .set("type", "hjemme")
                            .set("address1", "Ajervegen 2")
                            .build(),
                            new GenericRecordBuilder(addressSchema)
                            .set("type", "hytta")
                            .set("address1", "Meksikovegen 893")
                            .build()
                            ))
                    .build())
                .set("Event", new GenericRecordBuilder(addressRemovedSchema)
                        .set("Address", new GenericRecordBuilder(addressSchema)
                            .set("type", "hytta")
                            .build())
                    .build())
                .build();

        schemaRegistry.register(TOPIC+ "-value", customerSchema);
        serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"), false);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));

        System.out.println(schemaAndValue.value());

    }

    @Test
    public void testComplex() throws RestClientException, IOException {
        Map<String, String> map = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                new AbstractMap.SimpleImmutableEntry<>("schema.names", "ComplexSchemaName"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INCLUDENAMESPACE, "true")
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        converter.configure(map, false);
        org.apache.avro.Schema subrecord2Schema = org.apache.avro.SchemaBuilder.record("subrecord2").fields()
                .optionalInt("int32")
                .endRecord();

        org.apache.avro.Schema subrecord1Schema = org.apache.avro.SchemaBuilder.record("subrecord1").fields()
                .name("subrecord2").type(subrecord2Schema)
                .noDefault()
                .name("array").type().array().items().unionOf()
                .nullType().and().stringType().endUnion()
                .noDefault()
                .endRecord();

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName")
                .fields()
                .nullableInt("int8", 2)
                .requiredInt("int16")
                .requiredInt("int32")
                .requiredInt("int64")
                .requiredFloat("float32")
                .requiredBoolean("boolean")
                .optionalString("string")
                .requiredBytes("bytes")
                .name("array").type().array().items().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
                .name("map").type().map().values().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)).noDefault()
                .name("subrecord1").type(subrecord1Schema).noDefault()
                .endRecord()
                ;

        System.out.println(avroSchema);

        GenericData.Record struct = new GenericRecordBuilder(avroSchema)
                .set("int8", 120)
                .set("int16", 120)
                .set("int32", 120)
                .set("int64", 120L)
                .set("float32", 120.2f)
                .set("boolean", true)
                .set("string", "stringyåøæ¤#&|§")
                .set("bytes", ByteBuffer.wrap("foo".getBytes()))
                .set("array", Arrays.asList("a", "b", "c"))
                .set("map", Collections.singletonMap("field", 1))
                .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                        .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                                .set("int32", 199)
                                .build())
                        .set("array", Collections.singletonList("x"))
                        .build())
                .build();

        SchemaBuilder expectedBuilder = SchemaBuilder.struct()
                .field("intkey", SchemaBuilder.int8().doc("int8 field").build())
                .field("stringkey", Schema.STRING_SCHEMA)
                .field("event", Schema.STRING_SCHEMA);
        // Because of registration in schema registry and lookup, we'll have added a version number
        Schema expectedSchema = expectedBuilder.version(1).build();
        Struct expected = new Struct(expectedSchema)
                .put("intkey", (byte) 120)
                .put("stringkey", "stringyåøæ¤#&|§")
                .put("event", struct.toString());

        schemaRegistry.register(TOPIC+ "-value", avroSchema);
        serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"), false);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));
        //assertEquals(expected, schemaAndValue.value());
        System.out.println(expected);
        System.out.println(schemaAndValue.value());
    }

    @Test
    public void testComplexRecordNameStrategy() throws RestClientException, IOException {
        Map<String, String> map = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName()),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
                new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.intkey", "intkey"),
                new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.keyname", "stringkey"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INCLUDENAMESPACE, "true")
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        converter.configure(map, false);


        org.apache.avro.Schema subrecord2Schema = org.apache.avro.SchemaBuilder.record("subrecord2").fields()
                .optionalInt("int32")
                .endRecord();

        org.apache.avro.Schema subrecord1Schema = org.apache.avro.SchemaBuilder.record("subrecord1").fields()
                .name("subrecord2").type(subrecord2Schema)
                .noDefault()
                .name("array").type().array().items().unionOf()
                .nullType().and().stringType().endUnion()
                .noDefault()
                .endRecord();

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName")
                .fields()
                .nullableInt("int8", 2)
                .requiredInt("int16")
                .requiredInt("int32")
                .requiredInt("int64")
                .requiredFloat("float32")
                .requiredBoolean("boolean")
                .optionalString("string")
                .requiredBytes("bytes")
                .name("array").type().array().items().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
                .name("map").type().map().values().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)).noDefault()
                .name("subrecord1").type(subrecord1Schema).noDefault()
                .endRecord()
                ;

        System.out.println(avroSchema);

        GenericData.Record struct = new GenericRecordBuilder(avroSchema)
                .set("int8", 112)
                .set("int16", 112)
                .set("int32", 112)
                .set("int64", 112L)
                .set("float32", 112.2f)
                .set("boolean", true)
                .set("string", "stringyåøæ¤#&|§")
                .set("bytes", ByteBuffer.wrap("foo".getBytes()))
                .set("array", Arrays.asList("a", "b", "c"))
                .set("map", Collections.singletonMap("field", 1))
                .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                        .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                                .set("int32", 199)
                                .build())
                        .set("array", Collections.singletonList("x"))
                        .build())
                .build();

        org.apache.avro.Schema simpleAvroSchema = org.apache.avro.SchemaBuilder.record("com.example.SimpleSchemaName")
                .fields()
                .nullableInt("int8", 2)
                .requiredInt("int16")
                .requiredInt("intkey")
                .optionalString("keyname")
                .requiredLong("int64")
                .endRecord();

        System.out.println(simpleAvroSchema);

        GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
                .set("int8", 112)
                .set("int16", 112)
                .set("intkey", 112)
                .set("keyname", "stringyåøæ¤#&|§")
                .set("int64", 112L)
                .build();

        SchemaBuilder expectedBuilder = SchemaBuilder.struct()
                .field("intkey", SchemaBuilder.int8().doc("int8 field").build())
                .field("stringkey", Schema.STRING_SCHEMA)
                .field("event", Schema.STRING_SCHEMA);
        // Because of registration in schema registry and lookup, we'll have added a version number
        Schema expectedSchema = expectedBuilder.version(1).build();

        Struct expected = new Struct(expectedSchema)
                .put("intkey", (byte) 112)
                .put("stringkey", "stringyåøæ¤#&|§")
                .put("event", struct.toString());

        Map<String, String> smap = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
        schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);
        serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(smap, false);

        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));
        //assertEquals(expected, schemaAndValue.value());
        System.out.println(expected);
        System.out.println(schemaAndValue.value());

        SchemaAndValue schemaAndValueSimple = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));
        //assertEquals(expected, schemaAndValue.value());
        System.out.println(expected);
        System.out.println(schemaAndValueSimple.value());
    }

    @Test
    public void testComplexJSON() {
        String complexJSON =  "" +
                "{\"int8\": 12,\n" +
                "\"int16\": 12,\n" +
                "\"int32\": 12,\n" +
                "\"int64\": 12,\n" +
                "\"float32\": 12.2,\n" +
                "\"float64\": 12.2,\n" +
                "\"boolean\": true,\n" +
                "\"string\": \"stringy\",\n" +
                "\"array\": [\"a\", \"b\", \"c\"],\n" +
                "\"map\": {\"field\": 1},\n" +
                "\"mapNonStringKeys\": {1:1},\n" +
                "}";
        Map<String, Object> map = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("schema.names", "ComplexSchemaName"),
                new AbstractMap.SimpleImmutableEntry<>("json.ComplexSchemaName.int32", "true"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "json")
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        converter.configure(map, false);

        SchemaBuilder expectedBuilder = SchemaBuilder.struct()
                .field("intkey", SchemaBuilder.int8().doc("int8 field").build())
                .field("stringkey", Schema.STRING_SCHEMA)
                .field("event", Schema.STRING_SCHEMA);
        // Because of registration in schema registry and lookup, we'll have added a version number
        Schema expectedSchema = expectedBuilder.version(1).build();
        Struct expected = new Struct(expectedSchema)
                .put("intkey", (byte) 12)
                .put("stringkey", "stringy")
                .put("event", complexJSON);

        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, complexJSON.getBytes());
        //assertEquals(expected, schemaAndValue.value());
        System.out.println(expected);
        System.out.println(schemaAndValue.value());
    }

    @Test
    public void testCacheReuseOnMultipleComplex() throws RestClientException, IOException {
        Map<String, String> map = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                new AbstractMap.SimpleImmutableEntry<>("schema.names", "ComplexSchemaName"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        converter.configure(map, false);

        org.apache.avro.Schema subrecord2Schema = org.apache.avro.SchemaBuilder.record("subrecord2").fields()
                .optionalInt("int32")
                .endRecord();

        org.apache.avro.Schema subrecord1Schema = org.apache.avro.SchemaBuilder.record("subrecord1").fields()
                .name("subrecord2").type(subrecord2Schema)
                .noDefault()
                .name("array").type().array().items().unionOf()
                .nullType().and().stringType().endUnion()
                .noDefault()
                .endRecord();

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName")
                .fields()
                .nullableInt("int8", 2)
                .requiredInt("int16")
                .requiredInt("int32")
                .requiredInt("int64")
                .requiredFloat("float32")
                .requiredBoolean("boolean")
                .optionalString("string")
                .requiredBytes("bytes")
                .name("array").type().array().items().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
                .name("map").type().map().values().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)).noDefault()
                .name("subrecord1").type(subrecord1Schema).noDefault()
                .endRecord()
                ;

        System.out.println(avroSchema);

        GenericData.Record struct = new GenericRecordBuilder(avroSchema)
                .set("int8", 12)
                .set("int16", 12)
                .set("int32", 12)
                .set("int64", 12L)
                .set("float32", 12.2f)
                .set("boolean", true)
                .set("string", "stringyåøæ¤#&|§")
                .set("bytes", ByteBuffer.wrap("foo".getBytes()))
                .set("array", Arrays.asList("a", "b", "c"))
                .set("map", Collections.singletonMap("field", 1))
                .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                        .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                                .set("int32", 199)
                                .build())
                        .set("array", Collections.singletonList("x"))
                        .build())
                .build();


        SchemaBuilder expectedBuilder = SchemaBuilder.struct()
                .field("intkey", SchemaBuilder.int8().doc("int8 field").build())
                .field("stringkey", Schema.STRING_SCHEMA)
                .field("event", Schema.STRING_SCHEMA);
        // Because of registration in schema registry and lookup, we'll have added a version number
        Schema expectedSchema = expectedBuilder.version(1).build();
        Struct expected = new Struct(expectedSchema)
                .put("intkey", (byte) 12)
                .put("stringkey", "stringy")
                .put("event", struct.toString());

        schemaRegistry.register(TOPIC+ "-value", avroSchema);
        serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"), false);


        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));
        System.out.println(expected);
        System.out.println(schemaAndValue.value());
        SchemaAndValue schemaAndValue2 = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));
        System.out.println(expected);
        System.out.println(schemaAndValue2.value());
        SchemaAndValue schemaAndValue3 = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));
        System.out.println(expected);
        System.out.println(schemaAndValue3.value());
        //assertEquals(expected, schemaAndValue.value());
    }

    @Test
    public void testComplexArrayItemAsKey() throws RestClientException, IOException {
        Map<String, String> map = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                new AbstractMap.SimpleImmutableEntry<>("schema.names", "ComplexSchemaName"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.array", "arraykey"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        converter.configure(map, false);
        org.apache.avro.Schema subrecord2Schema = org.apache.avro.SchemaBuilder.record("subrecord2").fields()
                .optionalInt("int32")
                .endRecord();

        org.apache.avro.Schema subrecord1Schema = org.apache.avro.SchemaBuilder.record("subrecord1").fields()
                .name("subrecord2").type(subrecord2Schema)
                .noDefault()
                .name("array").type().array().items().unionOf()
                .nullType().and().stringType().endUnion()
                .noDefault()
                .endRecord();

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName")
                .fields()
                .nullableInt("int8", 2)
                .requiredInt("int16")
                .requiredInt("int32")
                .requiredInt("int64")
                .requiredFloat("float32")
                .requiredBoolean("boolean")
                .optionalString("string")
                .requiredBytes("bytes")
                .name("array").type().array().items().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
                .name("map").type().map().values().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)).noDefault()
                .name("subrecord1").type(subrecord1Schema).noDefault()
                .endRecord()
                ;
        System.out.println(avroSchema);

        GenericData.Record struct = new GenericRecordBuilder(avroSchema)
                .set("int8", 12)
                .set("int16", 12)
                .set("int32", 12)
                .set("int64", 12L)
                .set("float32", 12.2f)
                .set("boolean", true)
                .set("string", "stringyåøæ¤#&|§")
                .set("bytes", ByteBuffer.wrap("foo".getBytes()))
                .set("array", Arrays.asList("a", "b", "c"))
                .set("map", Collections.singletonMap("field", 1))
                .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                        .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                                .set("int32", 199)
                                .build())
                        .set("array", Collections.singletonList("x"))
                        .build())
                .build();

        SchemaBuilder expectedBuilder = SchemaBuilder.struct()
                .field("intkey", SchemaBuilder.int8().doc("int8 field").build())
                .field("arraykey", Schema.STRING_SCHEMA)
                .field("stringkey", Schema.STRING_SCHEMA)
                .field("event", Schema.STRING_SCHEMA);
        // Because of registration in schema registry and lookup, we'll have added a version number
        Schema expectedSchema = expectedBuilder.version(1).build();
        Struct expected = new Struct(expectedSchema)
                .put("intkey", (byte) 12)
                .put("arraykey", "a")
                .put("stringkey", "stringyåøæ¤#&|§")
                .put("event", struct.toString());

        schemaRegistry.register(TOPIC+ "-value", avroSchema);
        serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"), false);


        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));
        //assertEquals(expected, schemaAndValue.value());
        System.out.println(expected);
        System.out.println(schemaAndValue.value());
    }

    @Test
    public void testLogicalType() throws RestClientException, IOException {
        Map<String, String> map = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                new AbstractMap.SimpleImmutableEntry<>("schema.names", "ComplexSchemaName"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.date", "datekey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.time", "timekey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.timestamp", "timestampkey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.decimal", "decimalkey"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        converter.configure(map, false);

        org.apache.avro.Schema dateSchema = org.apache.avro.SchemaBuilder.builder().intType();
        LogicalTypes.date().addToSchema(dateSchema);

        org.apache.avro.Schema timeSchema = org.apache.avro.SchemaBuilder.builder().intType();
        LogicalTypes.timeMillis().addToSchema(timeSchema);

        org.apache.avro.Schema timemSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.timeMicros().addToSchema(timemSchema);

        org.apache.avro.Schema tsSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.timestampMillis().addToSchema(tsSchema);

        org.apache.avro.Schema tsmSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.timestampMicros().addToSchema(tsmSchema);

        org.apache.avro.Schema ltsSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.localTimestampMillis().addToSchema(ltsSchema);

        org.apache.avro.Schema ltsmSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.localTimestampMicros().addToSchema(ltsmSchema);

        org.apache.avro.Schema decimalSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        LogicalTypes.decimal(64, 0).addToSchema(decimalSchema);

        org.apache.avro.Schema uuidSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        LogicalTypes.uuid().addToSchema(uuidSchema);

        int dateDefValue = 100;
        int timeDefValue = 1000 * 60 * 60 * 2;
        long tsDefValue = 1000 * 60 * 60 * 24 * 365 + 100;
        String uuidDefValue = UUID.randomUUID().toString();
        java.util.Date dateDef = Date.toLogical(Date.SCHEMA, dateDefValue);
        java.util.Date timeDef = Time.toLogical(Time.SCHEMA, timeDefValue);
        java.util.Date tsDef = Timestamp.toLogical(Timestamp.SCHEMA, tsDefValue);
        BigDecimal decimalDef = new BigDecimal(BigInteger.valueOf(314159L), 0);
        byte[] decimalDefVal = decimalDef.unscaledValue().toByteArray();

        org.apache.avro.Schema subrecord2Schema = org.apache.avro.SchemaBuilder.record("subrecord2").fields()
                .name("optionaldate").type().unionOf().nullType().and().type(dateSchema).endUnion().nullDefault()
                .name("optionaltime").type().unionOf().nullType().and().type(timeSchema).endUnion().nullDefault()
                .name("optionaltimemicros").type().unionOf().nullType().and().type(timemSchema).endUnion().nullDefault()
                .name("optionaltimestamp").type().unionOf().nullType().and().type(tsSchema).endUnion().nullDefault()
                .name("optionaltimestampmicros").type().unionOf().nullType().and().type(tsmSchema).endUnion().nullDefault()
                .name("optionallocaltimestamp").type().unionOf().nullType().and().type(ltsSchema).endUnion().nullDefault()
                .name("optionallocaltimestampmicros").type().unionOf().nullType().and().type(ltsmSchema).endUnion().nullDefault()
                .name("optionaldecimal").type().unionOf().nullType().and().type(decimalSchema).endUnion().nullDefault()
                .name("optionaluuid").type().unionOf().nullType().and().type(uuidSchema).endUnion().nullDefault()
                .endRecord();

        org.apache.avro.Schema subrecord1Schema = org.apache.avro.SchemaBuilder.record("subrecord1").fields()
                .name("date_default").type(dateSchema).withDefault(dateDefValue)
                .name("time_default").type(timeSchema).withDefault(timeDefValue)
                .name("timemicros_default").type(timemSchema).withDefault(timeDefValue)
                .name("timestamp_default").type(tsSchema).withDefault(tsDefValue)
                .name("timestampmicros_default").type(tsmSchema).withDefault(tsDefValue)
                .name("localtimestamp_default").type(ltsSchema).withDefault(tsDefValue)
                .name("localtimestampmicros_default").type(ltsmSchema).withDefault(tsDefValue)
                .name("decimal_default").type(decimalSchema).withDefault(decimalDefVal)
                .name("uuid_default").type(uuidSchema).withDefault(uuidDefValue)
                .name("subrecord2").type(subrecord2Schema).noDefault()
                .endRecord();

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName")
                .fields()
                .name("date").type(dateSchema).noDefault()
                .name("time").type(timeSchema).noDefault()
                .name("timemicros").type(timemSchema).noDefault()
                .name("timestamp").type(tsSchema).noDefault()
                .name("timestampmicros").type(tsmSchema).noDefault()
                .name("localtimestamp").type(ltsSchema).noDefault()
                .name("localtimestampmicros").type(ltsmSchema).noDefault()
                .name("decimal").type(decimalSchema).noDefault()
                .name("uuid").type(uuidSchema).noDefault()
                .name("subrecord1").type(subrecord1Schema).noDefault()
                .endRecord()
                ;

        System.out.println(avroSchema);

        GenericData.Record subrecordUnionStruct = new GenericRecordBuilder(subrecord2Schema)
                .set("optionaldate", dateDefValue)
                .set("optionaltime", null)
                .set("optionaltimemicros", tsDefValue)
                .set("optionaltimestamp", null)
                .set("optionaltimestampmicros", tsDefValue)
                .set("optionallocaltimestamp", null)
                .set("optionallocaltimestampmicros", tsDefValue)
                .set("optionaldecimal", null)
                .set("optionaluuid", uuidDefValue)
                .build();

        GenericData.Record subrecordDefaultsstruct = new GenericRecordBuilder(subrecord1Schema)
                .set("subrecord2", subrecordUnionStruct)
                .build();

        GenericData.Record struct = new GenericRecordBuilder(avroSchema)
                .set("date", dateDefValue)
                .set("time", timeDefValue)
                .set("timemicros", tsDefValue)
                .set("timestamp", tsDefValue)
                .set("timestampmicros", tsDefValue)
                .set("localtimestamp", tsDefValue)
                .set("localtimestampmicros", tsDefValue)
                .set("decimal", ByteBuffer.wrap(decimalDefVal))
                .set("uuid", uuidDefValue)
                .set("subrecord1", subrecordDefaultsstruct)
                .build();

        /*SchemaBuilder expectedBuilder = SchemaBuilder.struct()
                .field("datekey", Schema.STRING_SCHEMA)
                .field("timekey", Schema.STRING_SCHEMA)
                .field("timestampkey", Schema.STRING_SCHEMA)
                .field("decimalkey", Schema.STRING_SCHEMA)
                .field("event", Schema.STRING_SCHEMA);
        // Because of registration in schema registry and lookup, we'll have added a version number
        Schema expectedSchema = expectedBuilder.version(1).build();*/

        /*GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(avroSchema);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroSchema, outputStream);
        encoder.setIncludeNamespace(false);
        datumWriter.write(struct, encoder);
        encoder.flush();
        outputStream.close();*/


        /*ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(new Conversions.UUIDConversion());
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
        //genericData.addLogicalTypeConversion(new JsonConverter.JDateConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema, genericData);
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroSchema, outputStream);
        writer.write(struct, encoder);
        encoder.flush();
        outputStream.close();*/

        /*final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema, avroSchema, genericData);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord event = reader.read(null, decoder);*/

        /*Struct expected = new Struct(expectedSchema)
                .put("datekey", dateDef.toString())
                .put("timekey", timeDef.toString())
                .put("timestampkey", tsDef.toString())
                .put("decimalkey", decimalDef.toString())
                .put("event", new String(outputStream.toByteArray(), StandardCharsets.UTF_8));

        System.out.println(expected);*/

        schemaRegistry.register(TOPIC+ "-value", avroSchema);
        serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"), false);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));
        //assertEquals(expected, schemaAndValue.value());
        //System.out.println(expected);
        System.out.println(schemaAndValue.value());
    }

    @Test
    public void testUTF8() throws RestClientException, IOException {
        Map<String, String> map = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                new AbstractMap.SimpleImmutableEntry<>("schema.names", "ComplexSchemaName"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
                new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
                new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        converter.configure(map, false);

        org.apache.avro.Schema subrecord2Schema = org.apache.avro.SchemaBuilder.record("subrecord2").fields()
                .optionalInt("int32")
                .endRecord();

        org.apache.avro.Schema subrecord1Schema = org.apache.avro.SchemaBuilder.record("subrecord1").fields()
                .name("subrecord2").type(subrecord2Schema)
                .noDefault()
                .name("array").type().array().items().unionOf()
                .nullType().and().stringType().endUnion()
                .noDefault()
                .endRecord();

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName")
                .fields()
                .nullableInt("int8", 2)
                .requiredInt("int16")
                .requiredInt("int32")
                .requiredInt("int64")
                .requiredFloat("float32")
                .requiredBoolean("boolean")
                .optionalString("string")
                .requiredBytes("bytes")
                .name("array").type().array().items().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
                .name("map").type().map().values().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)).noDefault()
                .name("subrecord1").type(subrecord1Schema).noDefault()
                .endRecord()
                ;

        System.out.println(avroSchema);

        GenericData.Record struct = new GenericRecordBuilder(avroSchema)
                .set("int8", 12)
                .set("int16", 12)
                .set("int32", 12)
                .set("int64", 12L)
                .set("float32", 12.2f)
                .set("boolean", true)
                .set("string", "stringyåøæ¤#&|§Ҋ ҈Ҏ")
                .set("bytes", ByteBuffer.wrap("foo".getBytes()))
                .set("array", Arrays.asList("a", "b", "c"))
                .set("map", Collections.singletonMap("field", 1))
                .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                        .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                                .set("int32", 199)
                                .build())
                        .set("array", Collections.singletonList("x"))
                        .build())
                .build();

        SchemaBuilder expectedBuilder = SchemaBuilder.struct()
                .field("intkey", SchemaBuilder.int8().doc("int8 field").build())
                .field("stringkey", Schema.STRING_SCHEMA)
                .field("event", Schema.STRING_SCHEMA);
        // Because of registration in schema registry and lookup, we'll have added a version number
        Schema expectedSchema = expectedBuilder.version(1).build();
        Struct expected = new Struct(expectedSchema)
                .put("intkey", (byte) 12)
                .put("stringkey", "stringyåøæ¤#&|§Ҋ ҈Ҏ")
                .put("event", struct.toString());

        schemaRegistry.register(TOPIC+ "-value", avroSchema);
        serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"), false);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));
        //assertEquals(expected, schemaAndValue.value());
        System.out.println(expected);
        System.out.println(schemaAndValue.value());
    }

    @Test
    public void testSchemaLogicalTypeToStringConversion() {
        org.apache.avro.Schema dateSchema = org.apache.avro.SchemaBuilder.builder().intType();
        LogicalTypes.date().addToSchema(dateSchema);

        org.apache.avro.Schema timeSchema = org.apache.avro.SchemaBuilder.builder().intType();
        LogicalTypes.timeMillis().addToSchema(timeSchema);

        org.apache.avro.Schema timemSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.timeMicros().addToSchema(timemSchema);

        org.apache.avro.Schema tsSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.timestampMillis().addToSchema(tsSchema);

        org.apache.avro.Schema tsmSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.timestampMicros().addToSchema(tsmSchema);

        org.apache.avro.Schema ltsSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.localTimestampMillis().addToSchema(ltsSchema);

        org.apache.avro.Schema ltsmSchema = org.apache.avro.SchemaBuilder.builder().longType();
        LogicalTypes.localTimestampMicros().addToSchema(ltsmSchema);

        org.apache.avro.Schema decimalSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        LogicalTypes.decimal(64, 0).addToSchema(decimalSchema);

        org.apache.avro.Schema uuidSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        LogicalTypes.uuid().addToSchema(uuidSchema);

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName")
                .fields()
                .name("date").type(dateSchema).noDefault()
                .name("time").type(timeSchema).noDefault()
                .name("timemicros").type(timemSchema).noDefault()
                .name("timestamp").type(tsSchema).noDefault()
                .name("timestampmicros").type(tsmSchema).noDefault()
                .name("localtimestamp").type(ltsSchema).noDefault()
                .name("localtimestampmicros").type(ltsmSchema).noDefault()
                .name("decimal").type(decimalSchema).noDefault()
                .name("uuid").type(uuidSchema).noDefault()
                .endRecord();

        org.apache.avro.Schema tgtSchema = createLogicalTypesStringSchema(avroSchema);

        Assert.assertEquals("{\"type\":\"record\",\"name\":\"ComplexSchemaName\",\"fields\":[{\"name\":\"date\",\"type\":{\"type\":\"string\",\"logicalType\":\"jsondate\"}},{\"name\":\"time\",\"type\":{\"type\":\"string\",\"logicalType\":\"jsontime-millis\"}},{\"name\":\"timemicros\",\"type\":{\"type\":\"string\",\"logicalType\":\"jsontime-micros\"}},{\"name\":\"timestamp\",\"type\":{\"type\":\"string\",\"logicalType\":\"jsontimestamp-millis\"}},{\"name\":\"timestampmicros\",\"type\":{\"type\":\"string\",\"logicalType\":\"jsontimestamp-micros\"}},{\"name\":\"localtimestamp\",\"type\":{\"type\":\"string\",\"logicalType\":\"jsonlocal-timestamp-millis\"}},{\"name\":\"localtimestampmicros\",\"type\":{\"type\":\"string\",\"logicalType\":\"jsonlocal-timestamp-micros\"}},{\"name\":\"decimal\",\"type\":{\"type\":\"string\",\"logicalType\":\"jsondecimal\"}},{\"name\":\"uuid\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}]}",
                tgtSchema.toString());
    }
}
