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
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.*;
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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        // ...configuration that decides which part of the original record is to be converted to json string and which
        // are to become seperate column values
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
        //Create target schema based on configuration schema.names prefixed mapping rules in the config decide which columns
        //will be added to the targetschema. They will all be added as an optional string field to the target schema.
        //One column will always be added with name as configured in payload.field.name and type mandatory string filed.
        targetSchema = createTargetSchema();
        logger.info("Target schema {}", targetSchema.fields());

        //In case of a source topic with avro messages the schema registry gets set up to support deserializing into Java GenericRecord
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
            LogicalTypes.register(JSONDATE, new JSONDateFactory());
            LogicalTypes.register(JSONDECIMAL, new JSONDecimalFactory());
            LogicalTypes.register(JSONTIME_MICROS, new JSONTimeMicrosFactory());
            LogicalTypes.register(JSONLOCAL_TIMESTAMP_MICROS, new JSONLocalTimestampMicrosFactory());
            LogicalTypes.register(JSONLOCAL_TIMESTAMP_MILLIS, new JSONLocalTimestampMillisFactory());
            LogicalTypes.register(JSONTIME_MILLIS, new JSONTimeMillisFactory());
            LogicalTypes.register(JSONTIMESTAMP_MICROS, new JSONTimestampMicrosFactory());
            LogicalTypes.register(JSONTIMESTAMP_MILLIS, new JSONTimestampMillisFactory());
            //AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG is by default configured to true for this converter.
            //This is to assure that the io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer gets configured to convert logicaltypes to their Java data type representation
            //indirectly this sets up the genericData is configured with logical type conversions as passed to GenericDatumReader(writerSchema, finalReaderSchema, genericData)
            Map<String, ?> newMap = Stream.concat(configs.entrySet().stream(), Stream.of(new AbstractMap.SimpleEntry<>(KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            // Pass the SchemaRegistryClient to the Deserializer and AvroData classes
            deserializer = new Deserializer(newMap, schemaRegistry);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        throw new ConfigException("Converter can only be used to convert to Connect Data");
    }

    //From a Kafka record value in bytes to a Kafka Connect Schema and Value that can be passed to the Kafka Connect Sink connector
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (jsonConverterConfig == null) {
            throw new ConfigException("Converter must be configured before it can serialize to avro");
        }
        //Branch into logic that deals with an avro source topic vs a json source topic
        switch (jsonConverterConfig.getInputFormat()) {
            case "avro":
                return toConnectDataFromAvro(topic, value);
            case "json":
                return toConnectDataFromJson(topic, value);
            default:
                throw new ConfigException(String.format("%s is not supported as %s by this converter", jsonConverterConfig.getInputFormat(), JsonConverterConfig.INPUT_FORMAT));
        }
    }

    //Create target schema based on configuration schema.names prefixed mapping rules in the config decide which columns
    //will be added to the targetschema. They will all be added as an optional string field to the target schema.
    //One column will always be added with name as configured in payload.field.name and type mandatory string filed.
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
                jsonConverterConfig.ucase(jsonConverterConfig.getPayloadFieldName()),
                Schema.STRING_SCHEMA
        );
        Schema newSchema = targetSchema.build();
        logger.trace("Created new schema {}", newSchema);
        return newSchema;
    }

    //Create a list of lambda instructions that can be cached per source structure consumed from the source topic
    //Instructions that convert source structures to a target Kafka Connect Schema and Value with optional String fields
    //that carry some pre-configured keys that are extracted from the payload to seperate fields (to allow indexing in the target table)
    //and a mandatory String field that carries the complete source message as JSON string
    private ExtractInstruction getExtractInstruction(CacheRequest cr) {
        //instructions differ from avro processing vs json processing
        switch (jsonConverterConfig.getInputFormat()) {
            case "avro":
                logger.debug("Creating instructions to cache for new schema {}", ((GenericContainer) cr.value).getSchema());
                if (cr.value instanceof IndexedRecord) {
                    //get the record object that triggered this instruction creation step
                    IndexedRecord deserialized = (IndexedRecord) cr.value;
                    //Create new list of instructions
                    List<Instruction> gettingKeyFields = new ArrayList<>();
                    //If the converter is configured with any keys...i.e. "keys." prefix in config...
                    Optional.ofNullable(jsonConverterConfig.getKeys())
                            //get the configured key renaming pairs associated to the schema name of this record object
                            .map(keys -> keys.get(deserialized.getSchema().getName()))
                            //if none, no instructions need to be created for key field to column extraction
                            .orElse(Collections.EMPTY_MAP)
                            //For each configured key that need to be extracted
                            .forEach((oldname, newname) -> {
                                //..build the lambda instruction that extracts the configured key from the record structure with method getSchemaField
                                Instruction avroSchemaInstruction = getSchemaField(((String) oldname).split("\\."), (String) oldname, (String) newname, deserialized.getSchema(), o -> o);
                                //Add this instruction to the instruction list
                                gettingKeyFields.add(avroSchemaInstruction);
                            });

                    //JSONENCODER
                    org.apache.avro.Schema jsonLogicalTypedSchema = createLogicalTypesStringSchema(((GenericRecord) cr.value).getSchema());
                    logger.info("Generated schema that will be used for JSON serialization: \n {}", jsonLogicalTypedSchema);
                    final GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(jsonLogicalTypedSchema);
                    writer.getData().addLogicalTypeConversion(new JSONDateConversion());
                    writer.getData().addLogicalTypeConversion(new JSONDecimalConversion());
                    writer.getData().addLogicalTypeConversion(new JSONLocalTimestampMillisConversion());
                    writer.getData().addLogicalTypeConversion(new JSONLocalTimestampMicrosConversion());
                    writer.getData().addLogicalTypeConversion(new JSONTimestampMillisConversion());
                    writer.getData().addLogicalTypeConversion(new JSONTimestampMicrosConversion());
                    writer.getData().addLogicalTypeConversion(new JSONTimeMillisConversion());
                    writer.getData().addLogicalTypeConversion(new JSONTimeMicrosConversion());
                    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    final JsonEncoder encoder = getJsonEncoder(jsonLogicalTypedSchema, outputStream);
                    encoder.setIncludeNamespace(jsonConverterConfig.getIncludeNamespace());

                    //Incorporate the key field extraction instructions in a bigger lambda that creates a Kafka connect target Struct...
                    //...applies each of the key extraction instructions i.e. adding them to the target Struct
                    //...and finally serializes the full avro record object that arrives from the source topic to a JSON string and puts it in the target Struct
                    return new ExtractInstruction(o -> {
                        Struct targetStruct = new Struct(targetSchema);
                        // Set the configured .keys prefixed fields with their values
                        gettingKeyFields.forEach(ai -> targetStruct.put(ai.newName, ai.instruction.apply(o)));

                        try {
                            outputStream.reset();
                            writer.write(((GenericRecord) o), encoder);
                            encoder.flush();
                            targetStruct.put(jsonConverterConfig.ucase(jsonConverterConfig.getPayloadFieldName()), new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
                        } catch (IOException e) {
                            throw new DataException(String.format("Could not write avro record to json string \n: %s", cr.value), e);
                        } finally {
                            try {
                                outputStream.close();
                            } catch (IOException e) {
                                throw new DataException(String.format("Could not close output stream when processing avro value \n: %s", cr.value), e);
                            }
                        }
                        //targetStruct.put(jsonConverterConfig.getPayloadFieldName(), new String(outputStream.toByteArray(), StandardCharsets.UTF_8));

                        return new SchemaAndValue(targetSchema, targetStruct);
                    });
                }
                throw new DataException(
                        String.format("Failed to build instructions to cache for record value %s: ", cr.value)
                );
            case "json":
                logger.debug("Creating instructions to cache for target schema {}", cr.value);
                //Create new list of instructions
                List<Instruction> gettingKeyFields = new ArrayList<>();
                String originalSchema = (String ) cr.value;
                //If the converter is configured with any keys...i.e. "keys." prefix in config...
                Optional.ofNullable(jsonConverterConfig.getKeys())
                        //get the configured key renaming pairs associated to the schema identifier of this JSON object
                        .map(keys -> keys.get(originalSchema))
                        //if none, no instructions need to be created for key field to column extraction
                        .orElse(Collections.EMPTY_MAP)
                        //For each configured key that need to be extracted
                        .forEach((oldname, newname) -> {
                            //..build the lambda instruction that extracts the configured key from the JSON object with method getJsonFieldInstruction
                            Instruction jsonSchemaInstruction = getJsonFieldInstruction(((String) oldname).split("\\."), (String) oldname, (String) newname, jo -> jo);
                            //Add this instruction to the instruction list
                            gettingKeyFields.add(jsonSchemaInstruction);
                        });
                //Incorporate the key field extraction instructions in a bigger lambda that creates a Kafka connect target Struct...
                //...applies each of the key extraction instructions i.e. adding them to the target Struct
                //...and finally serializes the full avro record object that arrives from the source topic to a JSON string and puts it in the target Struct
                return new ExtractInstruction(o -> {

                    Struct targetStruct = new Struct(targetSchema);
                    // Set the configured .keys prefixed fields with their values
                    gettingKeyFields.forEach(si -> targetStruct.put(si.newName, si.instruction.apply(o)));

                    // Set the "payload.field.name" field with the raw message as JSON with a simple .toString() of the JSON object
                    targetStruct.put(jsonConverterConfig.ucase(jsonConverterConfig.getPayloadFieldName()), o.toString());

                    return new SchemaAndValue(targetSchema, targetStruct);
                });
            default:
                throw new ConfigException(String.format("%s is not supported as %s by this converter", jsonConverterConfig.getInputFormat(), JsonConverterConfig.INPUT_FORMAT));
        }
    }

    private JsonEncoder getJsonEncoder(org.apache.avro.Schema jsonLogicalTypedSchema, ByteArrayOutputStream outputStream) {
        final JsonEncoder encoder;
        try {
            encoder = EncoderFactory.get().jsonEncoder(jsonLogicalTypedSchema, outputStream);
        } catch (IOException e) {
            throw new DataException(String.format("Could not build a jsonEncoder for avro record \n: %s", jsonLogicalTypedSchema), e);
        }
        return encoder;
    }

    //Identify source structure and apply associated cached instructions that produce the target Kafka Connect Schema and Value
    public SchemaAndValue toConnectDataFromJson(String topic, byte[] value) {
        //Null value
        if (value == null) {
            return SchemaAndValue.NULL;
        }
        String jsonString = new String(value, StandardCharsets.UTF_8);
        //If value is a JSON Object
        try {
            JSONObject jsonObject = new JSONObject(jsonString);
            String schema = null;

            // Test all schema identifiers that are configured e.g. the following configuration "json.ComplexSchemaName.int32", "true"...
            // is a test that the source structure has a field with name int32...
            // if so the schema is identified as schema with name ComplexSchemaName
            for (Map.Entry<String, AbstractMap.SimpleEntry<String, Object>> schemaEntry : jsonConverterConfig.getSchemaIdentifiers().entrySet()) {
                logger.debug("Checking if schemaIdentifier {} applies to object \n {}", schemaEntry, jsonObject);
                //..identify schema for the field that matches the schema identifier path given in config with method identifySchema
                if (identifySchema(schemaEntry.getValue().getKey().split("\\."), schemaEntry.getValue().getValue(), jsonObject)) {
                    schema = schemaEntry.getKey();
                    break;
                }
            }
            //All structures that are encountered on the source topic need to be pre-configured with identifier rules
            //this assures mapping rules for each source schema to the target event store table schema
            //i.e. which fields, if any, end up as seperate columns the events can be indexed on
            if (schema == null) {
                throw new DataException(String.format("The kafka record JSON value: %s \n could not be identified with the configured instructions %s :",
                        jsonObject,
                        jsonConverterConfig.getSchemaIdentifiers()));
            }
            //Apply the already cached extraction and json serialization instructions for this schema
            //if none are cached, due to an empty cache or a newly encountered schema, new instructions will be populated by method getExtractInstruction
            //which is configured to be called in the extractInstructionCache
            return extractInstructionCache.get(new CacheRequest(schema.hashCode(), schema))
                    .getConverterFunction().apply(jsonObject);
        } catch (JSONException e) {
          /*if source structures with an array as root or a primitive instead of a root object would need to be handled the logic can be extended here
            }*/
            throw new DataException(
                    String.format("Failed to parse JSON data for topic %s: ", topic),
                    e
            );
        } catch (ExecutionException e) {
            throw new DataException(
                    String.format("Failed to fetch instructions from cache for record value %s: ", new String(value)),
                    e
            );
        }
    }

    //Identify source structure and apply associated cached instructions that produce the target Kafka Connect Schema and Value
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

            //If value is a Generic record
            if (deserialized instanceof IndexedRecord) {
                logger.trace("Deserialized avro {}", deserialized);
                //All schema names that are encountered on the source topic need to be pre-configured
                //this assures mapping rules for each schema to the target event store table schema
                //i.e. which fields, if any, end up as seperate columns the events can be indexed on
                if (!jsonConverterConfig.getSchemaNames().contains(deserialized.getSchema().getName())) {
                    throw new DataException(String.format("The schema name: %s, is not configured in %s for config: %s",
                            deserialized.getSchema().getName(),
                            jsonConverterConfig.getSchemaNames(),
                            SCHEMA_NAMES
                    ));
                }
                //Apply the already cached extraction and json serialization instructions for this schema
                //if none are cached, due to an empty cache or a newly encountered schema, new instructions will be populated by method getExtractInstruction
                //which is configured to be called in the extractInstructionCache
                return extractInstructionCache.get(new CacheRequest(deserialized.getSchema().hashCode(), deserialized))
                        .getConverterFunction().apply(deserialized);
            } /*if source structures with an array as root or a primitive instead of a root record would need to be handled the logic can be extended here
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

    //Build the lambda instruction that extracts the configured key from the JSON object
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
                        } else if (gr instanceof JSONArray) {
                            Object eo;
                            try {
                                eo = ((JSONArray) gr).get(0);
                            } catch (JSONException e) {
                                eo = null;
                            }
                            return eo;
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
                    } else if (gr instanceof JSONArray) {
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

    //Identify schema for the field that matches the schema identifier path given in config with method identifySchema
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
                return identifySchema(Arrays.copyOfRange(fieldnames, 1, fieldnames.length), test, o);
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

    public static org.apache.avro.Schema createLogicalTypesStringSchema(org.apache.avro.Schema originalSchema) {
        switch (originalSchema.getType()) {
            case RECORD:
                return org.apache.avro.Schema.createRecord(originalSchema.getName(), originalSchema.getDoc(), originalSchema.getNamespace(), originalSchema.isError(),
                        originalSchema.getFields().stream()
                                .map(f -> new org.apache.avro.Schema.Field(f.name(), createLogicalTypesStringSchema(f.schema()), f.doc(), null, f.order()))
                                .collect(Collectors.toList())
                );
            case ENUM:
                return org.apache.avro.Schema.createEnum(originalSchema.getName(), originalSchema.getDoc(), originalSchema.getNamespace(), originalSchema.getEnumSymbols(), originalSchema.getEnumDefault());
            case ARRAY:
                return org.apache.avro.Schema.createArray(createLogicalTypesStringSchema(originalSchema.getElementType()));
            case MAP:
                return org.apache.avro.Schema.createMap(createLogicalTypesStringSchema(originalSchema.getValueType()));
            case UNION:
                return org.apache.avro.Schema.createUnion(
                        originalSchema.getTypes().stream()
                                .map(JsonConverter::createLogicalTypesStringSchema)
                                .collect(Collectors.toList())
                );
            case INT:
            case LONG:
            case FIXED:
            case BYTES:
                if (originalSchema.getLogicalType() != null) {
                    org.apache.avro.Schema stringSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
                    LogicalType lt;
                    switch (originalSchema.getLogicalType().getName()){
                        case "date":
                            lt = new JSONDate();
                            lt.addToSchema(stringSchema);
                            break;
                        case "decimal":
                            lt = new JSONDecimal(((LogicalTypes.Decimal )originalSchema.getLogicalType()).getPrecision(),
                                    ((LogicalTypes.Decimal )originalSchema.getLogicalType()).getScale());
                            lt.addToSchema(stringSchema);
                            break;
                        case "time-millis":
                            lt = new JSONTimeMillis();
                            lt.addToSchema(stringSchema);
                            break;
                        case "time-micros":
                            lt = new JSONTimeMicros();
                            lt.addToSchema(stringSchema);
                            break;
                        case "timestamp-millis":
                            lt = new JSONTimestampMillis();
                            lt.addToSchema(stringSchema);
                            break;
                        case "timestamp-micros":
                            lt = new JSONTimestampMicros();
                            lt.addToSchema(stringSchema);
                            break;
                        case "local-timestamp-millis":
                            lt = new JSONLocalTimestampMillis();
                            lt.addToSchema(stringSchema);
                            break;
                        case "local-timestamp-micros":
                            lt = new JSONLocalTimestampMicros();
                            lt.addToSchema(stringSchema);
                            break;
                        default:
                            throw new UnknownFormatConversionException(
                                String.format("Can't convert the following format to a JSON string %s", originalSchema.getLogicalType().getName()));
                    }
                    return stringSchema;
                }
                return originalSchema;
            case STRING:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case NULL:
                return originalSchema;
            default:
                throw new DataException(String.format("Trying to create a target schema where timestamp related logical types are of type String" +
                        " to support JSON serialization, however, this failed for type %s", originalSchema));
        }
    }

    //Build the lambda instruction that extracts the configured key from the avro record object
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

        private final int schemahash;
        private final Object value;

        private CacheRequest(int schemahash, Object value) {
            this.schemahash = schemahash;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            CacheRequest that = (CacheRequest) o;
            return schemahash == (that.schemahash);
        }

        @Override
        public int hashCode() {
            return schemahash;
        }

        public int getSchemaHash() {
            return schemahash;
        }

        public Object getValue() {
            return value;
        }
    }

    public static class JSONLocalTimestampMicros extends LogicalType {
        private JSONLocalTimestampMicros() {
            super(JSONLOCAL_TIMESTAMP_MICROS);
        }

        public void validate(org.apache.avro.Schema schema) {
            super.validate(schema);
            if (schema.getType() != org.apache.avro.Schema.Type.STRING) {
                throw new IllegalArgumentException("Local timestamp (micros) can only be used with an underlying string type");
            }
        }
    }

    public static class JSONLocalTimestampMillis extends LogicalType {
        private JSONLocalTimestampMillis() {
            super(JSONLOCAL_TIMESTAMP_MILLIS);
        }

        public void validate(org.apache.avro.Schema schema) {
            super.validate(schema);
            if (schema.getType() != org.apache.avro.Schema.Type.STRING) {
                throw new IllegalArgumentException("Local timestamp (millis) can only be used with an underlying string type");
            }
        }
    }

    public static class JSONTimestampMicros extends LogicalType {
        private JSONTimestampMicros() {
            super(JSONTIMESTAMP_MICROS);
        }

        public void validate(org.apache.avro.Schema schema) {
            super.validate(schema);
            if (schema.getType() != org.apache.avro.Schema.Type.STRING) {
                throw new IllegalArgumentException("Timestamp (micros) can only be used with an underlying string type");
            }
        }
    }

    public static class JSONTimestampMillis extends LogicalType {
        private JSONTimestampMillis() {
            super(JSONTIMESTAMP_MILLIS);
        }

        public void validate(org.apache.avro.Schema schema) {
            super.validate(schema);
            if (schema.getType() != org.apache.avro.Schema.Type.STRING) {
                throw new IllegalArgumentException("Timestamp (millis) can only be used with an underlying string type");
            }
        }
    }

    public static class JSONTimeMicros extends LogicalType {
        private JSONTimeMicros() {
            super(JSONTIME_MICROS);
        }

        public void validate(org.apache.avro.Schema schema) {
            super.validate(schema);
            if (schema.getType() != org.apache.avro.Schema.Type.STRING) {
                throw new IllegalArgumentException("Time (micros) can only be used with an underlying string type");
            }
        }
    }

    public static class JSONTimeMillis extends LogicalType {
        private JSONTimeMillis() {
            super(JSONTIME_MILLIS);
        }

        public void validate(org.apache.avro.Schema schema) {
            super.validate(schema);
            if (schema.getType() != org.apache.avro.Schema.Type.STRING) {
                throw new IllegalArgumentException("Time (millis) can only be used with an underlying string type");
            }
        }
    }

    public static class JSONDate extends LogicalType {
        private JSONDate() {
            super(JSONDATE);
        }

        public void validate(org.apache.avro.Schema schema) {
            super.validate(schema);
            if (schema.getType() != org.apache.avro.Schema.Type.STRING) {
                throw new IllegalArgumentException("Date can only be used with an underlying string type");
            }
        }
    }

    public static class JSONDecimal extends LogicalType {
        private static final String PRECISION_PROP = "precision";
        private static final String SCALE_PROP = "scale";
        private final int precision;
        private final int scale;

        private JSONDecimal(int precision, int scale) {
            super(JSONDECIMAL);
            this.precision = precision;
            this.scale = scale;
        }

        private JSONDecimal(org.apache.avro.Schema schema) {
            super(JSONDECIMAL);
            if (!this.hasProperty(schema, "precision")) {
                throw new IllegalArgumentException("Invalid decimal: missing precision");
            } else {
                this.precision = this.getInt(schema, "precision");
                if (this.hasProperty(schema, "scale")) {
                    this.scale = this.getInt(schema, "scale");
                } else {
                    this.scale = 0;
                }

            }
        }
        private boolean hasProperty(org.apache.avro.Schema schema, String name) {
            return schema.getObjectProp(name) != null;
        }

        private int getInt(org.apache.avro.Schema schema, String name) {
            Object obj = schema.getObjectProp(name);
            if (obj instanceof Integer) {
                return (Integer)obj;
            } else {
                throw new IllegalArgumentException("Expected int " + name + ": " + (obj == null ? "null" : obj + ":" + obj.getClass().getSimpleName()));
            }
        }
    }

    public static final String JSONDECIMAL = "jsondecimal";
    public static final String JSONDATE = "jsondate";
    public static final String JSONTIME_MILLIS = "jsontime-millis";
    public static final String JSONTIME_MICROS = "jsontime-micros";
    public static final String JSONTIMESTAMP_MILLIS = "jsontimestamp-millis";
    public static final String JSONTIMESTAMP_MICROS = "jsontimestamp-micros";
    public static final String JSONLOCAL_TIMESTAMP_MILLIS = "jsonlocal-timestamp-millis";
    public static final String JSONLOCAL_TIMESTAMP_MICROS = "jsonlocal-timestamp-micros";

    public static class JSONLocalTimestampMicrosFactory implements LogicalTypes.LogicalTypeFactory {
        private JSONLocalTimestampMicros type = new JSONLocalTimestampMicros();
        @Override
        public LogicalType fromSchema(org.apache.avro.Schema schema) {
            return type;
        }
    }
    public static class JSONLocalTimestampMillisFactory implements LogicalTypes.LogicalTypeFactory {
        private JSONLocalTimestampMillis type = new JSONLocalTimestampMillis();
        @Override
        public LogicalType fromSchema(org.apache.avro.Schema schema) {
            return type;
        }
    }
    public static class JSONTimestampMicrosFactory implements LogicalTypes.LogicalTypeFactory {
        private JSONTimestampMicros type = new JSONTimestampMicros();
        @Override
        public LogicalType fromSchema(org.apache.avro.Schema schema) {
            return type;
        }
    }
    public static class JSONTimestampMillisFactory implements LogicalTypes.LogicalTypeFactory {
        private JSONTimestampMillis type = new JSONTimestampMillis();
        @Override
        public LogicalType fromSchema(org.apache.avro.Schema schema) {
            return type;
        }
    }
    public static class JSONTimeMicrosFactory implements LogicalTypes.LogicalTypeFactory {
        private JSONTimeMicros type = new JSONTimeMicros();
        @Override
        public LogicalType fromSchema(org.apache.avro.Schema schema) {
            return type;
        }
    }
    public static class JSONTimeMillisFactory implements LogicalTypes.LogicalTypeFactory {
        private JSONTimeMillis type = new JSONTimeMillis();
        @Override
        public LogicalType fromSchema(org.apache.avro.Schema schema) {
            return type;
        }
    }
    public static class JSONDateFactory implements LogicalTypes.LogicalTypeFactory {
        private JSONDate type = new JSONDate();
        @Override
        public LogicalType fromSchema(org.apache.avro.Schema schema) {
            return type;
        }
    }
    public static class JSONDecimalFactory implements LogicalTypes.LogicalTypeFactory {
        @Override
        public LogicalType fromSchema(org.apache.avro.Schema schema) {
            return new JSONDecimal(schema);
        }
    }

    public static class JSONLocalTimestampMicrosConversion extends Conversion<LocalDateTime> {

        public JSONLocalTimestampMicrosConversion() {
        }

        public Class<LocalDateTime> getConvertedType() {
            return LocalDateTime.class;
        }

        public String getLogicalTypeName() {
            return JSONLOCAL_TIMESTAMP_MICROS;
        }

        public LocalDateTime fromCharSequence(String dt, org.apache.avro.Schema schema, LogicalType type) {
            throw new UnknownFormatConversionException("This conversion only supports to String conversions");
        }

        public String toCharSequence(LocalDateTime timestamp, org.apache.avro.Schema schema, LogicalType type) {
            return timestamp.toString();
        }

        public org.apache.avro.Schema getRecommendedSchema() {
            return LogicalTypes.localTimestampMicros().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        }
    }

    public static class JSONLocalTimestampMillisConversion extends Conversion<LocalDateTime> {

        public JSONLocalTimestampMillisConversion() {
        }

        public Class<LocalDateTime> getConvertedType() {
            return LocalDateTime.class;
        }

        public String getLogicalTypeName() {
            return JSONLOCAL_TIMESTAMP_MILLIS;
        }

        public LocalDateTime fromCharSequence(String dt, org.apache.avro.Schema schema, LogicalType type) {
            throw new UnknownFormatConversionException("This conversion only supports to String conversions");
        }

        public String toCharSequence(LocalDateTime timestamp, org.apache.avro.Schema schema, LogicalType type) {
            return timestamp.toString();
        }

        public org.apache.avro.Schema getRecommendedSchema() {
            return LogicalTypes.localTimestampMillis().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        }
    }

    public static class JSONTimestampMicrosConversion extends Conversion<Instant> {

        public JSONTimestampMicrosConversion() {
        }

        public Class<Instant> getConvertedType() {
            return Instant.class;
        }

        public String getLogicalTypeName() {
            return JSONTIMESTAMP_MICROS;
        }

        public Instant fromCharSequence(String dt, org.apache.avro.Schema schema, LogicalType type) {
            throw new UnknownFormatConversionException("This conversion only supports to String conversions");
        }

        public String toCharSequence(Instant instant, org.apache.avro.Schema schema, LogicalType type) {
            return instant.toString();
        }

        public org.apache.avro.Schema getRecommendedSchema() {
            return LogicalTypes.timestampMicros().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        }
    }

    public static class JSONTimestampMillisConversion extends Conversion<Instant> {

        public JSONTimestampMillisConversion() {
        }

        public Class<Instant> getConvertedType() {
            return Instant.class;
        }

        public String getLogicalTypeName() {
            return JSONTIMESTAMP_MILLIS;
        }

        public Instant fromCharSequence(String dt, org.apache.avro.Schema schema, LogicalType type) {
            throw new UnknownFormatConversionException("This conversion only supports to String conversions");
        }

        public String toCharSequence(Instant timestamp, org.apache.avro.Schema schema, LogicalType type) {
            return timestamp.toString();
        }

        public org.apache.avro.Schema getRecommendedSchema() {
            return LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        }
    }

    public static class JSONTimeMicrosConversion extends Conversion<LocalTime> {

        public JSONTimeMicrosConversion() {
        }

        public Class<LocalTime> getConvertedType() {
            return LocalTime.class;
        }

        public String getLogicalTypeName() {
            return JSONTIME_MICROS;
        }

        public LocalTime fromCharSequence(String dt, org.apache.avro.Schema schema, LogicalType type) {
            throw new UnknownFormatConversionException("This conversion only supports to String conversions");
        }

        public String toCharSequence(LocalTime time, org.apache.avro.Schema schema, LogicalType type) {
            return time.toString();
        }

        public org.apache.avro.Schema getRecommendedSchema() {
            return LogicalTypes.timeMicros().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        }
    }

    public static class JSONTimeMillisConversion extends Conversion<LocalTime> {

        public JSONTimeMillisConversion() {
        }

        public Class<LocalTime> getConvertedType() {
            return LocalTime.class;
        }

        public String getLogicalTypeName() {
            return JSONTIME_MILLIS;
        }

        public LocalTime fromCharSequence(String dt, org.apache.avro.Schema schema, LogicalType type) {
            throw new UnknownFormatConversionException("This conversion only supports to String conversions");
        }

        public String toCharSequence(LocalTime time, org.apache.avro.Schema schema, LogicalType type) {
            return time.toString();
        }

        public org.apache.avro.Schema getRecommendedSchema() {
            return LogicalTypes.timeMillis().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        }
    }

    public static class JSONDateConversion extends Conversion<LocalDate> {

        public JSONDateConversion() {
        }

        public Class<LocalDate> getConvertedType() {
            return LocalDate.class;
        }

        public String getLogicalTypeName() {
            return JSONDATE;
        }

        public LocalDate fromCharSequence(String dt, org.apache.avro.Schema schema, LogicalType type) {
            throw new UnknownFormatConversionException("This conversion only supports to String conversions");
        }

        public String toCharSequence(LocalDate date, org.apache.avro.Schema schema, LogicalType type) {
            return date.toString();
        }

        public org.apache.avro.Schema getRecommendedSchema() {
            return LogicalTypes.date().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        }
    }

    public static class JSONDecimalConversion extends Conversion<BigDecimal> {

        public JSONDecimalConversion() {
        }

        public Class<BigDecimal> getConvertedType() {
            return BigDecimal.class;
        }

        public org.apache.avro.Schema getRecommendedSchema() {
            throw new UnsupportedOperationException("No recommended schema for decimal (scale is required)");
        }

        public String getLogicalTypeName() {
            return JSONDECIMAL;
        }

        public BigDecimal fromCharSequence(String value, org.apache.avro.Schema schema, LogicalType type) {
            throw new UnknownFormatConversionException("This conversion only supports to String conversions");
        }

        public String toCharSequence(BigDecimal value, org.apache.avro.Schema schema, LogicalType type) {
            return value.toString();
        }
    }
}
