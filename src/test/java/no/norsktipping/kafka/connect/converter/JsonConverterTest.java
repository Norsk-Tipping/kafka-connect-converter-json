package no.norsktipping.kafka.connect.converter;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// jsonConverter is a trivial combination of the serializers and the AvroData conversions, so
// most testing is performed on AvroData since it is much easier to compare the results in Avro
// runtime format than in serialized form. This just adds a few sanity checks to make sure things
// work end-to-end.
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

    //TODO: Refactor the commented tests
/*    @Test
    public void testConfigure() {
        converter.configure(SR_CONFIG, true);
        assertTrue(Whitebox.<Boolean>getInternalState(converter, "isKey"));
        assertNotNull(Whitebox.getInternalState(
                Whitebox.<AbstractKafkaSchemaSerDe>getInternalState(converter, "serializer"),
                "schemaRegistry"));
    }*/

/*    @Test
    public void testConfigureAlt() {
        converter.configure(SR_CONFIG, false);
        assertFalse(Whitebox.<Boolean>getInternalState(converter, "isKey"));
        assertNotNull(Whitebox.getInternalState(
                Whitebox.<AbstractKafkaSchemaSerDe>getInternalState(converter, "serializer"),
                "schemaRegistry"));
    }*/

/*    @Test
    public void testPrimitive() {
        Map<String, String> map = Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
                new AbstractMap.SimpleImmutableEntry<>("keys.int32", "test"),
                new AbstractMap.SimpleImmutableEntry<>("keys.string", "test")
        )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        converter.configure(map, false);
        SchemaAndValue original = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
        byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original.value());
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
        // Because of registration in schema registry and lookup, we'll have added a version number
        SchemaAndValue expected = new SchemaAndValue(SchemaBuilder.bool().version(1).build(), true);
        assertEquals(expected, schemaAndValue);
    }*/

    @Test
    public void testComplex() throws RestClientException, IOException {
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
                .set("int8", 12)
                .set("int16", 12)
                .set("intkey", 12)
                .set("keyname", "stringyåøæ¤#&|§")
                .set("int64", 12L)
                .build();

        SchemaBuilder expectedBuilder = SchemaBuilder.struct()
                .field("intkey", SchemaBuilder.int8().doc("int8 field").build())
                .field("stringkey", Schema.STRING_SCHEMA)
                .field("event", Schema.STRING_SCHEMA);
        // Because of registration in schema registry and lookup, we'll have added a version number
        Schema expectedSchema = expectedBuilder.version(1).build();

        Struct expected = new Struct(expectedSchema)
                .put("intkey", (byte) 12)
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

/*    @Test
    public void testTypeBytes() {
        Schema schema = SchemaBuilder.bytes().build();
        byte[] b = converter.fromConnectData("topic", schema, "jellomellow".getBytes());
        SchemaAndValue sv = converter.toConnectData("topic", b);
        assertEquals(Type.BYTES, sv.schema().type());
        assertArrayEquals("jellomellow".getBytes(), ((ByteBuffer) sv.value()).array());
    }*/

/*    @Test
    public void testNull() {
        // Because of the way our serialization works, it's expected that we'll lose schema information
        // when the entire schema is optional. The null value should be written as a null and this
        // should mean we also do *not* register a schema.
        byte[] converted = converter.fromConnectData(TOPIC, Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
        assertNull(converted);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
        assertEquals(SchemaAndValue.NULL, schemaAndValue);
    }*/

/*    @Test
    public void testVersionExtractedForDefaultSubjectNameStrategy() throws Exception {
        // Version info should be extracted even if the data was not created with Copycat. Manually
        // register a few compatible schemas and validate that data serialized with our normal
        // serializer can be read and gets version info inserted
        String subject = TOPIC + "-value";
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
        JsonConverter jsonConverter = new JsonConverter(schemaRegistry);
        jsonConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
        testVersionExtracted(subject, serializer, jsonConverter);

    }*/
/*
    @Test

    public void testVersionExtractedForRecordSubjectNameStrategy() throws Exception {
        // Version info should be extracted even if the data was not created with Copycat. Manually
        // register a few compatible schemas and validate that data serialized with our normal
        // serializer can be read and gets version info inserted
        String subject =  "Foo";
        Map<String, Object> configs = ImmutableMap.<String, Object>of("schema.registry.url", "http://fake-url", "value.subject.name.strategy", RecordNameStrategy.class.getName());
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(configs, false);
        JsonConverter jsonConverter = new JsonConverter(schemaRegistry);

        jsonConverter.configure(configs, false);
        testVersionExtracted(subject, serializer, jsonConverter);

    }
*/

/*    private void testVersionExtracted(String subject, KafkaAvroSerializer serializer, JsonConverter jsonConverter) throws IOException, RestClientException {
        // Pre-register to ensure ordering
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record("Foo").fields()
                .requiredInt("key")
                .endRecord();
        schemaRegistry.register(subject, new AvroSchema(avroSchema1));

        org.apache.avro.Schema avroSchema2 = org.apache.avro.SchemaBuilder
                .record("Foo").fields()
                .requiredInt("key")
                .requiredString("value")
                .endRecord();
        schemaRegistry.register(subject, new AvroSchema(avroSchema2));


        // Get serialized data
        org.apache.avro.generic.GenericRecord avroRecord1
                = new org.apache.avro.generic.GenericRecordBuilder(avroSchema1).set("key", 15).build();
        byte[] serializedRecord1 = serializer.serialize(TOPIC, avroRecord1);
        org.apache.avro.generic.GenericRecord avroRecord2
                = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2).set("key", 15).set
                ("value", "bar").build();
        byte[] serializedRecord2 = serializer.serialize(TOPIC, avroRecord2);


        SchemaAndValue converted1 = jsonConverter.toConnectData(TOPIC, serializedRecord1);
        assertEquals(1L, (long) converted1.schema().version());

        SchemaAndValue converted2 = jsonConverter.toConnectData(TOPIC, serializedRecord2);
        assertEquals(2L, (long) converted2.schema().version());
    }*/


/*    @Test
    public void testVersionMaintained() {
        // Version info provided from the Copycat schema should be maintained. This should be true
        // regardless of any underlying schema registry versioning since the versions are explicitly
        // specified by the connector.

        // Use newer schema first
        Schema newerSchema = SchemaBuilder.struct().version(2)
                .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
                .field("new", Schema.OPTIONAL_INT16_SCHEMA)
                .build();
        SchemaAndValue newer = new SchemaAndValue(newerSchema, new Struct(newerSchema));
        byte[] newerSerialized = converter.fromConnectData(TOPIC, newer.schema(), newer.value());

        Schema olderSchema = SchemaBuilder.struct().version(1)
                .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
                .build();
        SchemaAndValue older = new SchemaAndValue(olderSchema, new Struct(olderSchema));
        byte[] olderSerialized = converter.fromConnectData(TOPIC, older.schema(), older.value());

        assertEquals(2L, (long) converter.toConnectData(TOPIC, newerSerialized).schema().version());
        assertEquals(1L, (long) converter.toConnectData(TOPIC, olderSerialized).schema().version());
    }*/


/*    @Test
    public void testSameSchemaMultipleTopicForValue() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        JsonConverter jsonConverter = new JsonConverter(schemaRegistry);
        jsonConverter.configure(SR_CONFIG, false);
        assertSameSchemaMultipleTopic(jsonConverter, schemaRegistry, false);
    }*/

/*    @Test
    public void testSameSchemaMultipleTopicForKey() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        JsonConverter jsonConverter = new JsonConverter(schemaRegistry);
        jsonConverter.configure(SR_CONFIG, true);
        assertSameSchemaMultipleTopic(jsonConverter, schemaRegistry, true);
    }*/

/*    @Test
    public void testExplicitlyNamedNestedMapsWithNonStringKeys() {
        final Schema schema = SchemaBuilder.map(
                Schema.OPTIONAL_STRING_SCHEMA,
                SchemaBuilder.map(
                        Schema.OPTIONAL_STRING_SCHEMA,
                        Schema.INT32_SCHEMA
                ).name("foo.bar").build()
        ).name("biz.baz").version(1).build();
        final JsonConverter jsonConverter = new JsonConverter(new MockSchemaRegistryClient());
        jsonConverter.configure(
                Collections.singletonMap(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost"
                ),
                false
        );
        final Object value = Collections.singletonMap("foo", Collections.singletonMap("bar", 1));

        final byte[] bytes = jsonConverter.fromConnectData("topic", schema, value);
        final SchemaAndValue schemaAndValue = jsonConverter.toConnectData("topic", bytes);

        assertThat(schemaAndValue.schema(), equalTo(schema));
        assertThat(schemaAndValue.value(), equalTo(value));
    }*/

/*    private void assertSameSchemaMultipleTopic(JsonConverter converter, SchemaRegistryClient schemaRegistry, boolean isKey) throws IOException, RestClientException {
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record("Foo").fields()
                .requiredInt("key")
                .endRecord();

        org.apache.avro.Schema avroSchema2_1 = org.apache.avro.SchemaBuilder
                .record("Foo").fields()
                .requiredInt("key")
                .requiredString("value")
                .endRecord();
        org.apache.avro.Schema avroSchema2_2 = org.apache.avro.SchemaBuilder
                .record("Foo").fields()
                .requiredInt("key")
                .requiredString("value")
                .endRecord();
        String subjectSuffix = isKey ? "key" : "value";
        schemaRegistry.register("topic1-" + subjectSuffix, new AvroSchema(avroSchema2_1));
        schemaRegistry.register("topic2-" + subjectSuffix, new AvroSchema(avroSchema1));
        schemaRegistry.register("topic2-" + subjectSuffix, new AvroSchema(avroSchema2_2));

        org.apache.avro.generic.GenericRecord avroRecord1
                = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2_1).set("key", 15).set
                ("value", "bar").build();
        org.apache.avro.generic.GenericRecord avroRecord2
                = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2_2).set("key", 15).set
                ("value", "bar").build();


        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(SR_CONFIG, isKey);
        byte[] serializedRecord1 = serializer.serialize("topic1", avroRecord1);
        byte[] serializedRecord2 = serializer.serialize("topic2", avroRecord2);

        SchemaAndValue converted1 = converter.toConnectData("topic1", serializedRecord1);
        assertEquals(1L, (long) converted1.schema().version());

        SchemaAndValue converted2 = converter.toConnectData("topic2", serializedRecord2);
        assertEquals(2L, (long) converted2.schema().version());

        converted2 = converter.toConnectData("topic2", serializedRecord2);
        assertEquals(2L, (long) converted2.schema().version());
    }*/
}