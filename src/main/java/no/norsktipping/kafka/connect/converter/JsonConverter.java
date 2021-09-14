package no.norsktipping.kafka.connect.converter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static no.norsktipping.kafka.connect.converter.JsonConverterConfig.SCHEMA_NAMES;


/**
 * Implementation of Converter that uses Avro schemas and objects.
 */
public class JsonConverter implements Converter {
    private static final Logger logger = LoggerFactory.getLogger(JsonConverter.class);
    //Cache of 'field extraction' instructions per SchemaPair.
    private LoadingCache<CacheRequest, ExtractInstruction> extractInstructionCache;
    private JsonConverterConfig jsonConverterConfig;

    //Converting from Avro
    private SchemaRegistryClient schemaRegistry;
    private Deserializer deserializer;

    //Target schema to be handled by sink connector
    private Schema targetSchema;

    public JsonConverter() {
    }

    // Public only for testing
    public JsonConverter(SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Use other converters for Kafka record keys and SMTs to embed in connect struct when relevant
        if (isKey) {
            throw new ConfigException("JsonConverter can only be configured for record value");
        }

        // Config specific to this converter e.g. "keys.", "payload.field.name", "input.format"
        // ...configuration that decides which part of the original record is to be converted to json string and which maintain their specific
        // ...datatypes to allow these to become column values
        jsonConverterConfig = new JsonConverterConfig(configs);
        this.extractInstructionCache = CacheBuilder.newBuilder()
                .maximumSize(200)
                .build(
                        new CacheLoader<CacheRequest, ExtractInstruction>() {
                            public ExtractInstruction load(CacheRequest cr) throws Exception {
                                logger.debug("No instructions cached " +
                                        "for schema: {}", cr);
                                return getExtractInstruction(cr);
                            }
        });
        logger.info("Json converter is configured with the following keys to extract {}", jsonConverterConfig.getKeys());
        logger.info("Json converter is configured with the following input format {}", jsonConverterConfig.getInputFormat());
        targetSchema = createTargetSchema();
        logger.info("Target schema {}", targetSchema.fields());

        if (jsonConverterConfig.getInputFormat().equals("avro")) {
            // For deserializing from, as opposed to serializing to, Kafka this Json connector uses specific operations with Confluent's Deserializer rather than re-using
            // Confluent's converter, because it wants to override which elements from the source record are to be mapped to respective Kafka connect SchemaAndValue
            // It has therefore access to its own SchemaRegistryClient
            if (schemaRegistry == null) {
                schemaRegistry = new CachedSchemaRegistryClient(
                        jsonConverterConfig.getSchemaRegistryUrls(),
                        jsonConverterConfig.getMaxSchemasPerSubject(),
                        Collections.singletonList(new AvroSchemaProvider()),
                        configs,
                        jsonConverterConfig.requestHeaders()
                );
            }
            // Pass the SchemaRegistryClient to the Deserializer and AvroData classes
            deserializer = new Deserializer(configs, schemaRegistry);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        throw new ConfigException("Converter can only be used to convert to Connect Data");
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (jsonConverterConfig == null) {
            throw new ConfigException("Converter must be configured before it can serialize to avro");
        }
        switch (jsonConverterConfig.getInputFormat()) {
            case "avro":
                return toConnectDataFromAvro(topic, value);
            case "json":
                return toConnectDataFromJson(topic, value);
            default:
                throw new ConfigException(String.format("%s is not supported as %s by this converter", jsonConverterConfig.getInputFormat(), JsonConverterConfig.INPUT_FORMAT));
        }
    }

    private Schema createTargetSchema() {
        SchemaBuilder targetSchema = SchemaBuilder.struct().name("targetSchema");
        Optional.ofNullable(jsonConverterConfig.getKeys())
                .map(schemas ->
                        schemas.values().stream().flatMap(keys ->
                                keys.values().stream()).collect(Collectors.toSet())
                        )
                .orElse(new HashSet<>())
                .forEach(newName ->
                        targetSchema.field(
                            (String) newName,
                            Schema.OPTIONAL_STRING_SCHEMA
                    )
                );
        targetSchema.field(
                jsonConverterConfig.getPayloadFieldName(),
                Schema.STRING_SCHEMA
        );
        Schema newSchema = targetSchema.build();
        logger.trace("Created new schema {}", newSchema);
        return newSchema;
    }

    private ExtractInstruction getExtractInstruction(CacheRequest cr) {
        switch (jsonConverterConfig.getInputFormat()) {
            case "avro":
                logger.debug("Creating instructions to cache for new schema {}", cr.schema);
                if (cr.value instanceof IndexedRecord) {

                    IndexedRecord deserialized = (IndexedRecord) cr.value;
                    //Create new list of fields
                    List<Instruction> gettingKeyFields = new ArrayList<>();
                    //If the converter is configured with any keys...i.e. "keys." prefix in config...
                    Optional.ofNullable(jsonConverterConfig.getKeys())
                            .map(keys -> keys.get(deserialized.getSchema().getName()))
                            .orElse(Collections.EMPTY_MAP)
                            .forEach((oldname, newname) -> {
                                //..get schema for the field that matches the oldname path given in config
                                Instruction avroSchemaInstruction = getSchemaField(((String) oldname).split("\\."), (String) oldname, (String) newname, deserialized.getSchema(), o -> o);
                                gettingKeyFields.add(avroSchemaInstruction);
                            });

                    //JSONENCODER
                    GenericData genericData = new GenericData();
                    genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
                    genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());

                    final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(((GenericContainer) cr.value).getSchema(), genericData);
                    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    final JsonEncoder encoder = getJsonEncoder(cr, outputStream);
                    encoder.setIncludeNamespace(false);

                    return new ExtractInstruction(o -> {
                        Struct targetStruct = new Struct(targetSchema);
                        // Set the configured .keys prefixed fields with their values
                        gettingKeyFields.forEach(ai -> targetStruct.put(ai.newName, ai.instruction.apply(o)));
                        // Set the "payload.field.name" field with the raw message as JSON with a simple .toString() of the genericrecord

                        try {
                            outputStream.reset();
                            writer.write(((GenericRecord) o), encoder);
                            encoder.flush();
                            targetStruct.put(jsonConverterConfig.getPayloadFieldName(), new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
                        } catch (IOException e) {
                            throw new DataException(String.format("Could not write avro record to json string \n: %s", cr.value), e);
                        } finally {
                            try {
                                outputStream.close();
                            } catch (IOException e) {
                                throw new DataException(String.format("Could not close output stream when processing avro value \n: %s", cr.value), e);
                            }
                        }

                        return new SchemaAndValue(targetSchema, targetStruct);
                    });
                }
                throw new DataException(
                        String.format("Failed to build instructions to cache for record schema %s record value %s: ", cr.schema, cr.value)
                );
            case "json":
                logger.debug("Creating instructions to cache for target schema {}", cr.schema);
                //..generate instructions for keys
                //Create new list of fields
                List<Instruction> gettingKeyFields = new ArrayList<>();
                //If the converter is configured with any keys...i.e. "keys." prefix in config...
                String originalSchema = cr.schema;
                //Create new kafka connect struct of fields
                Optional.ofNullable(jsonConverterConfig.getKeys())
                        .map(keys -> keys.get(originalSchema))
                        .orElse(Collections.EMPTY_MAP)
                        .forEach((oldname, newname) -> {
                            //..get schema for the field that matches the oldname path given in config
                            Instruction jsonSchemaInstruction = getJsonFieldInstruction(((String) oldname).split("\\."), (String) oldname, (String) newname, jo -> jo);
                            //..add the field to the list of new fields, by the name configured in values for the respective "keys." prefix in config...
                            gettingKeyFields.add(jsonSchemaInstruction);
                        });
                return new ExtractInstruction(o -> {

                    Struct targetStruct = new Struct(targetSchema);
                    // Set the configured .keys prefixed fields with their values
                    gettingKeyFields.forEach(si -> targetStruct.put(si.newName, si.instruction.apply(o)));

                    // Set the "payload.field.name" field with the raw message as JSON with a simple .toString() of the json object
                    targetStruct.put(jsonConverterConfig.getPayloadFieldName(), o.toString());

                    return new SchemaAndValue(targetSchema, targetStruct);
                });
            default:
                throw new ConfigException(String.format("%s is not supported as %s by this converter", jsonConverterConfig.getInputFormat(), JsonConverterConfig.INPUT_FORMAT));
        }
    }

    private JsonEncoder getJsonEncoder(CacheRequest cr, ByteArrayOutputStream outputStream) {
        final JsonEncoder encoder;
        try {
            encoder = EncoderFactory.get().jsonEncoder(((IndexedRecord) cr.value).getSchema(), outputStream);
        } catch (IOException e) {
            throw new DataException(String.format("Could not build a jsonEncoder for avro record \n: %s", cr.value), e);
        }
        return encoder;
    }

    public SchemaAndValue toConnectDataFromJson(String topic, byte[] value) {
        //Null value
        if (value == null) {
            return SchemaAndValue.NULL;
        }
        String jsonString = new String(value, StandardCharsets.UTF_8);
        try {
            JSONObject jsonObject = new JSONObject(jsonString);
            String schema = null;
            for (Map.Entry<String, AbstractMap.SimpleEntry<String, Object>> schemaEntry : jsonConverterConfig.getSchemaIdentifiers().entrySet()) {
                logger.debug("Checking if schemaIdentifier {} applies to object \n {}", schemaEntry, jsonObject);
                //..identify schema for the field that matches the schema identifier path given in config
                if(identifySchema(schemaEntry.getValue().getKey().split("\\."), schemaEntry.getValue().getValue(), jsonObject)) {
                    schema = schemaEntry.getKey();
                    break;
                }
            }
            if (schema == null) {
                throw new DataException(String.format("The kafka record JSON value: %s \n could not be identified with the configured instructions %s :",
                        jsonObject,
                        jsonConverterConfig.getSchemaIdentifiers()));
            }
            return extractInstructionCache.get(new CacheRequest(schema, jsonObject))
                    .getConverterFunction().apply(jsonObject);
        } catch (JSONException e) {
            /*try {
                JSONArray jsonArray = new JSONArray(jsonString);
                SchemaBuilder newStructSchema = SchemaBuilder.struct().name("primitive");
                newStructSchema.field(
                        jsonConverterConfig.getPayloadFieldName(),
                        Schema.STRING_SCHEMA
                );
                Schema newSchema = newStructSchema.build();
                logger.trace("Created new primitive schema {}", newSchema);
                Struct newStruct = new Struct(newSchema);
                newStruct.put(jsonConverterConfig.getPayloadFieldName(), jsonArray.toString());

                return new SchemaAndValue(newSchema, newStruct);
            } catch (JSONException je) {*/
                throw new DataException(
                        String.format("Failed to parse JSON data for topic %s: ", topic),
                        e
                );
//            }
        } catch (ExecutionException e) {
            throw new DataException(
                    String.format("Failed to fetch instructions from cache for record value %s: ", new String(value)),
                    e
            );
        }
    }

    public SchemaAndValue toConnectDataFromAvro(String topic, byte[] value) {
        try {
            //Deserialize avro
            GenericContainerWithVersion containerWithVersion =
                    deserializer.deserialize(topic, false, value);
            //Null value
            if (containerWithVersion == null) {
                return SchemaAndValue.NULL;
            }
            GenericContainer deserialized = containerWithVersion.container();
            //If value is avro record
            if (deserialized instanceof IndexedRecord) {
                logger.trace("Deserialized avro {}", deserialized);

                if (!jsonConverterConfig.getSchemaNames().contains(deserialized.getSchema().getName())) {
                    throw new DataException(String.format("The schema name: %s, is not configured in %s for config: %s",
                            deserialized.getSchema().getName(),
                            jsonConverterConfig.getSchemaNames(),
                            SCHEMA_NAMES
                            ));
                }
                return extractInstructionCache.get(new CacheRequest(deserialized.getSchema().toString(), deserialized))
                        .getConverterFunction().apply(deserialized);
            } /*else if (deserialized instanceof NonRecordContainer) {
                SchemaBuilder newStructSchema = SchemaBuilder.struct().name("primitive");
                newStructSchema.field(
                        jsonConverterConfig.getPayloadFieldName(),
                        Schema.STRING_SCHEMA
                );
                Schema newSchema = newStructSchema.build();
                logger.trace("Created new primitive schema {}", newSchema);
                Struct newStruct = new Struct(newSchema);
                newStruct.put(jsonConverterConfig.getPayloadFieldName(), ((NonRecordContainer) deserialized).getValue().toString());

                return new SchemaAndValue(newSchema, newStruct);
            }*/
            throw new DataException(
                    String.format("Unsupported type returned during deserialization of topic %s ", topic)
            );
        } catch (SerializationException e) {
            throw new DataException(
                    String.format("Failed to deserialize data for topic %s to Avro: ", topic),
                    e
            );
        } catch (InvalidConfigurationException e) {
            throw new ConfigException(
                    String.format("Failed to access Avro data from topic %s :", topic),
                    e
            );
        } catch (ExecutionException e) {
            throw new DataException(
                    String.format("Failed to fetch instructions from cache for record value %s: ", new String(value)),
                    e
            );
        }
    }

    private Instruction getJsonFieldInstruction(String[] fieldnames, String oldName, String newName, Function<Object, Object> instruction) {

        if (fieldnames == null || fieldnames.length == 0) {
            throw new DataException("Fieldnames can not be empty");
        }
        if (fieldnames.length > 1) {
            instruction = instruction.andThen(o ->
                    Optional.ofNullable(o).map(gr -> {
                                if (gr instanceof JSONObject) {
                                    Object eo;
                                    try {
                                        eo = ((JSONObject) gr).get(fieldnames[0]);
                                    } catch (JSONException e) {
                                        eo = null;
                                    }
                                    return eo;
                                        //throw new DataException(String.format("The following key is not found in the JSON object: %s", fieldnames[0]));
                                    }
                                else if (gr instanceof JSONArray) {
                                    Object eo;
                                    try {
                                        eo = ((JSONArray) gr).get(0);
                                    } catch (JSONException e) {
                                        eo = null;
                                    }
                                    return eo;
                                    //throw new DataException(String.format("The following key is not found in the JSON object: %s", fieldnames[0]));
                                }
                                return null;
                                }).orElseThrow(() -> new DataException(
                            String.format("The following key is not found in the JSON object: %s", fieldnames[0])
                    )));
            return getJsonFieldInstruction(Arrays.copyOfRange(fieldnames, 1, fieldnames.length), fieldnames[1], newName, /*eo,*/ instruction);
        }
        instruction = instruction.andThen(o ->
                        Optional.ofNullable(o).map(gr -> {
                            if (gr instanceof JSONObject) {
                                Object eo;
                                try {
                                    eo = ((JSONObject) gr).get(fieldnames[0]);
                                    if (eo instanceof JSONArray) {
                                        try {
                                            eo = ((JSONArray) eo).get(0);
                                        } catch (JSONException e) {
                                            eo = null;
                                        }
                                    } else if (eo instanceof JSONObject) {
                                        try {
                                            eo = ((JSONObject) eo).get(((JSONObject) eo).keys().next());
                                        } catch (JSONException | NoSuchElementException e) {
                                            eo = null;
                                        }
                                    }
                                } catch (JSONException e) {
                                    eo = null;
                                }
                                return eo;
                            }
                            else if (gr instanceof JSONArray) {
                                Object eo;
                                try {
                                    eo = ((JSONArray) gr).get(0);
                                    if (eo instanceof JSONObject) {
                                        try {
                                            eo = ((JSONObject) eo).get(((JSONObject) eo).keys().next());
                                        } catch (JSONException | NoSuchElementException e) {
                                            eo = null;
                                        }
                                    } else if (eo instanceof JSONArray) {
                                        try {
                                            eo = ((JSONArray) eo).get(0);
                                        } catch (JSONException e) {
                                            eo = null;
                                        }
                                    }
                                } catch (JSONException e) {
                                    eo = null;
                                }
                                return eo;
                            } else {
                                return gr;
                            }
                        })
                        .map(Object::toString)
                .orElse(null)
        );
        return new Instruction(instruction, oldName, newName);
  }

  public long getCacheSize() {
        return extractInstructionCache.size();
  }

    private boolean identifySchema(String[] fieldnames, Object test, Object object) {
        logger.debug("Testing if {} applies to {} for object \n {}", test, fieldnames, object);
        if (fieldnames == null || fieldnames.length == 0) {
            throw new DataException("Fieldnames can not be empty");
        }
        if (object == null) {
            throw new DataException(String.format("The following key is not found in the JSON object: %s", fieldnames[0]));
        }
        if (object instanceof JSONObject) {
            Object o;
            try {
                o = ((JSONObject) object).get(fieldnames[0]);
            } catch (JSONException e) {
                return false;
            }
            if (fieldnames.length > 1) {
                return identifySchema(Arrays.copyOfRange(fieldnames, 1, fieldnames.length),  test, o);
            }
            if (test.equals("true")) {
                return true;
            }
            return o.toString().equals(test.toString());
        } else if (object instanceof JSONArray) {
            Object o;
            try {
                o = ((JSONArray) object).get(0);
            } catch (JSONException e) {
                return false;
            }
            if (fieldnames.length > 1) {
                return identifySchema(Arrays.copyOfRange(fieldnames, 1, fieldnames.length), test, o);
            }
            if (test.equals("true")) {
                return true;
            }
            return o.equals(test);
        }
        if (test.equals("true")) {
            return true;
        }
        return object.equals(test);
    }

    private Instruction getSchemaField(String[] fieldnames, String oldName, String newName, org.apache.avro.Schema schema, Function<Object, Object> instruction) {
        if (fieldnames == null || fieldnames.length == 0) {
            throw new DataException("Fieldnames can not be empty");
        }
        if (schema == null) {
            throw new DataException(String.format("The following key is not found in the Avro schema: %s", fieldnames[0]));
        }

        if (schema.getType() == org.apache.avro.Schema.Type.RECORD) {
            if (schema.getField(fieldnames[0]) == null) {
                throw new DataException(String.format("The following key is not found in the Avro schema: %s", fieldnames[0]));
            }
            if (fieldnames.length > 1) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o).map(gr -> ((GenericRecord) gr).get(fieldnames[0]))
                        .orElse(null)
                );
                return getSchemaField(Arrays.copyOfRange(fieldnames, 1, fieldnames.length), fieldnames[1], newName, schema.getField(fieldnames[0]).schema(), instruction);
            }
            if (schema.getField(fieldnames[0]).schema().getType() == org.apache.avro.Schema.Type.ARRAY) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o)
                                .map(gr -> ((GenericRecord) gr).get(fieldnames[0]))
                                .map(arr -> ((GenericArray<Object>) arr).get(0))
                                .map(Object::toString)
                        .orElse(null)
                );
            } else if (schema.getField(fieldnames[0]).schema().getType() == org.apache.avro.Schema.Type.MAP) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o)
                                .map(gr -> ((GenericRecord) gr).get(fieldnames[0]))
                                .map(m -> ((Map<Object, Object>) m))
                                .map(mm -> mm.values().stream().findFirst().orElse(null))
                                .map(Object::toString)
                                .orElse(null)
                );
            } else {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o)
                                .map(gr -> ((GenericRecord) gr).get(fieldnames[0]))
                                .map(Object::toString)
                        .orElse(null)
                );
            }
        } else if (schema.getType() == org.apache.avro.Schema.Type.ARRAY) {
            if (fieldnames.length > 1) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o)
                                .map(arr -> ((GenericArray<Object>) arr).get(0))
                                .orElse(null)
                );
                return getSchemaField(Arrays.copyOfRange(fieldnames, 1, fieldnames.length), fieldnames[1], newName, schema.getValueType(), instruction);
            }
            if (schema.getElementType().getType() == org.apache.avro.Schema.Type.ARRAY) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o)
                                .map(gr -> ((GenericArray<Object>) gr).get(0))
                                .map(arr -> ((GenericArray<Object>) arr).get(0))
                                .map(Object::toString)
                                .orElse(null)
                );
            } else if (schema.getElementType().getType() == org.apache.avro.Schema.Type.MAP) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o)
                                .map(gr -> ((GenericArray<Object>) gr).get(0))
                                .map(m -> ((Map<Object, Object>) m))
                                .map(mm -> mm.values().stream().findFirst().orElse(null))
                                .map(Object::toString)
                                .orElse(null)
                );
            } else {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o).map(arr ->
                                ((GenericArray<Object>) arr).get(0))
                                .map(Object::toString)
                                .orElse(null)
                );
            }
        } else if (schema.getType() == org.apache.avro.Schema.Type.MAP) {
            if (fieldnames.length > 1) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o).map(m ->
                                ((Map<Object, Object>) m))
                                .map(mm -> mm.values().stream().findFirst().orElse(null))
                                .orElse(null)
                        );
                return getSchemaField(Arrays.copyOfRange(fieldnames, 1, fieldnames.length), fieldnames[1], newName, schema.getValueType(), instruction);
            }
            if (schema.getValueType().getType() == org.apache.avro.Schema.Type.ARRAY) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o).map(m ->
                                ((Map<Object, Object>) m))
                                .map(mm -> mm.values().stream().findFirst().orElse(null))
                                .map(arr -> ((GenericArray<Object>) arr).get(0))
                                .map(Object::toString)
                                .orElse(null)
                );
            } else if (schema.getValueType().getType() == org.apache.avro.Schema.Type.MAP) {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o).map(m ->
                                ((Map<Object, Object>) m))
                                .map(mm -> mm.values().stream().findFirst().orElse(null))
                                .map(mmm -> ((Map<Object, Object>) mmm))
                                .map(mmmm -> mmmm.values().stream().findFirst().orElse(null))
                                .map(Object::toString)
                                .orElse(null)
                );
            } else {
                instruction = instruction.andThen(o ->
                        Optional.ofNullable(o).map(m ->
                                ((Map<Object, Object>) m))
                                .map(mm -> mm.values().stream().findFirst().orElse(null))
                                .map(Object::toString)
                                .orElse(null)
                );
            }
        }
        return new Instruction(instruction, oldName, newName);
    }

    private static class Instruction {
        private final Function<Object, Object> instruction;
        private final String oldName;
        private final String newName;

        private Instruction(Function<Object, Object> instruction, String oldName, String newName) {
            this.instruction = instruction;
            this.oldName = oldName;
            this.newName = newName;
        }

    }

    private static class Deserializer extends AbstractKafkaAvroDeserializer {

        public Deserializer(SchemaRegistryClient client) {
            schemaRegistry = client;
        }

        public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client);
            configure(new KafkaAvroDeserializerConfig(configs));
        }

        public GenericContainerWithVersion deserialize(String topic, boolean isKey, byte[] payload) {
            return deserializeWithSchemaAndVersion(topic, isKey, payload);
        }
    }

    private static class CacheRequest {

        private final String schema;
        private final Object value;

        private CacheRequest(String schema, Object value) {
            this.schema = schema;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheRequest that = (CacheRequest) o;
            return schema.equals(that.schema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema);
        }

        public String getSchema() {
            return schema;
        }

        public Object getValue() {
            return value;
        }
    }
}