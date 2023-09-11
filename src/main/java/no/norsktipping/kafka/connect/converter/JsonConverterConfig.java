package no.norsktipping.kafka.connect.converter;

import io.confluent.connect.avro.AvroConverterConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class JsonConverterConfig extends AbstractConfig {

    private AvroConverterConfig avroConverterConfig;
    public static final String PAYLOAD_FIELD_NAME = "payload.field.name";
    public static final String INPUT_FORMAT = "input.format";
    public static final String SCHEMA_NAMES = "schema.names";
    public static final String ALLOWNONINDEXED = "allownonindexed";
    public static final String UPPERCASE = "uppercase";
    public static final String INCLUDENAMESPACE = "includenamespace";

    private static  String payloadFieldName;
    private static  String inputFormat;
    private static Boolean allowNonIndexed;
    private static List<String> schemaNames;
    private static Boolean uppercase;
    private static Boolean includeNamespace;


    private final Map<String, Map<String, String>> keys;
    private Map<String, AbstractMap.SimpleEntry<String, Object>> schemaIdentifiers;
    private static final Logger log = LoggerFactory.getLogger(JsonConverterConfig.class);

    public JsonConverterConfig(Map<?, ?> props) {
        super(baseConfigDef(), props);
        payloadFieldName = payLoadFieldName();
        inputFormat = inputFormat();
        allowNonIndexed = allowNonIndexed();
        schemaNames = schemaNames();
        uppercase = uppercase();
        includeNamespace = includeNamespace();

        if(getInputFormat().equals("avro")) {
            avroConverterConfig = new AvroConverterConfig(props);
        }
        keys = keys();
        if(getInputFormat().equals("json")) {
            schemaIdentifiers = schemaIdentifiers();
        }
    }

    public static ConfigDef baseConfigDef() {

        ConfigDef configDef = (new ConfigDef())
                .define(PAYLOAD_FIELD_NAME, ConfigDef.Type.STRING, "event", ConfigDef.Importance.HIGH, "Specify the field name that will be used to contain the record value in JSON ")
                .define(INPUT_FORMAT, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(String s, Object o) {
                        if (!(o.equals("json") || o.equals("avro"))) {
                            throw new ConfigException(String.format("%s must either be json or avro", INPUT_FORMAT));
                        }
                    }
                }, ConfigDef.Importance.HIGH, "Specify the input format of the source topic the converter is used on: json/avro ")
                .define(SCHEMA_NAMES, ConfigDef.Type.LIST, new ArrayList<>(), ConfigDef.Importance.HIGH, "Specify the schema names expected on the source topic")
                .define(ALLOWNONINDEXED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "When true, Kafka records without matching schema in " + SCHEMA_NAMES
                        + " and without matching key instructions in config will be writting to a single column configured in " + PAYLOAD_FIELD_NAME + " other columns can be NULL")
                .define(UPPERCASE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, "Specify if target columns and table name are to be uppercase true or lowercase false ")
                .define(INCLUDENAMESPACE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "When true, produced JSON will include recordName.")
                ;
        return configDef;
    }

    private Map<String, Map<String, String>> keys() {
        Map<String, Map<String, String>> schemaKeyMapping = getSchemaNames().stream().map(schemaName ->
                new AbstractMap.SimpleEntry<>(
                        schemaName,
                        (Map) this.originalsWithPrefix(schemaName + ".").entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, (entry) -> ucase(Objects.toString(entry.getValue()))))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        schemaKeyMapping.values().forEach(m -> m.values().forEach(newKeyName -> {
            if (!schemaKeyMapping.values().stream().allMatch(m2 -> m2.values().stream().anyMatch(nkn -> nkn.equals(newKeyName)))) {
                if (!getAllowNonIndexed()) {
                    throw new ConfigException(String.format("Dereferencing %s must be defined for each schema that is configured in %s", newKeyName, SCHEMA_NAMES));
                } else {
                    log.info(String.format("Dereferencing %s is not defined for all keys for each schema that is configured in %s", newKeyName, SCHEMA_NAMES));
                }
            }
        }));
        return schemaKeyMapping;
    }
    public Map<String, Map<String, String>> getKeys() {
        return this.keys;
    }

    private Map<String, AbstractMap.SimpleEntry<String, Object>> schemaIdentifiers() {
        Map<String, AbstractMap.SimpleEntry<String, Object>> schemaIdentifiers = this.originalsWithPrefix("json" + ".").entrySet().stream().map(e ->
                new AbstractMap.SimpleEntry<>(StringUtils.substringBefore(e.getKey(), "."),
                        new AbstractMap.SimpleEntry<>(StringUtils.substringAfter(e.getKey(), "."), e.getValue())))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        schemaIdentifiers.forEach((key, value) -> {
            if (!getSchemaNames().contains(key)) {
                if (!getAllowNonIndexed()) {
                    throw new ConfigException(String.format("Each schema configured in %s must have a matching schema identifier rule configured, prefixed with %s \n" +
                            "schema name %s not found in %s", SCHEMA_NAMES, ".json.", key, SCHEMA_NAMES));
                } else {
                    log.info(String.format("Not each schema configured in %s has a matching schema identifier rule configured, prefixed with %s \n" +
                            "schema name %s not found in %s", SCHEMA_NAMES, ".json.", key, SCHEMA_NAMES));
                }
            }
        });
        if (getSchemaNames().size() != schemaIdentifiers.size()) {
            if (!getAllowNonIndexed()) {
                throw new ConfigException(String.format("Each schema configured in %s must have a matching schema identifier rule configured, prefixed with %s", SCHEMA_NAMES, ".json."));
            } else {
                log.info(String.format("Not each schema configured in %s has a matching schema identifier rule configured, prefixed with %s", SCHEMA_NAMES, ".json."));
            }
        }
        return schemaIdentifiers;
    }

    public Map<String, AbstractMap.SimpleEntry<String, Object>> getSchemaIdentifiers() {
        return this.schemaIdentifiers;
    }

    public String getPayloadFieldName() {
        return payloadFieldName;
    }
    private String payLoadFieldName(){
        return this.getString(PAYLOAD_FIELD_NAME);
    }

    private Boolean uppercase(){
        return this.getBoolean(UPPERCASE);
    }

    public String ucase(String string) {
        return uppercase ? string.toUpperCase() : string.toLowerCase();
    }

    public String getInputFormat() {
        return inputFormat;
    }
    private String inputFormat() {
        return this.getString(INPUT_FORMAT);
    }

    public Boolean getAllowNonIndexed() { return allowNonIndexed; }
    private Boolean allowNonIndexed() { return this.getBoolean(ALLOWNONINDEXED); }

    public List<String> getSchemaNames() {
        return schemaNames;
    }
    private List<String> schemaNames() {
        if (this.getList(SCHEMA_NAMES) == null || this.getList(SCHEMA_NAMES).isEmpty()) {
            throw new ConfigException(String.format("%s is missing which is required when %s is false (default)", SCHEMA_NAMES, ALLOWNONINDEXED));
        }
        return this.getList(SCHEMA_NAMES);
    }

    public Boolean getIncludeNamespace() {return includeNamespace;}
    private Boolean includeNamespace() {return this.getBoolean(INCLUDENAMESPACE); }

    public static class Builder {

        private Map<String, Object> props = new HashMap<>();

        public Builder with(String key, Object value) {
            props.put(key, value);
            return this;
        }

        public JsonConverterConfig build() {
            return new JsonConverterConfig(props);
        }
    }

    public int getMaxSchemasPerSubject() {

        return avroConverterConfig.getMaxSchemasPerSubject();
    }

    public List<String> getSchemaRegistryUrls() {

        return avroConverterConfig.getSchemaRegistryUrls();
    }

    public boolean autoRegisterSchema() {

        return avroConverterConfig.autoRegisterSchema();
    }

    public boolean useLatestVersion() {

        return avroConverterConfig.useLatestVersion();
    }

    public boolean getLatestCompatibilityStrict() {

        return avroConverterConfig.getLatestCompatibilityStrict();
    }

    public Object keySubjectNameStrategy() {

        return avroConverterConfig.keySubjectNameStrategy();
    }

    public Object valueSubjectNameStrategy() {

        return avroConverterConfig.valueSubjectNameStrategy();
    }

    public boolean useSchemaReflection() {

        return avroConverterConfig.useSchemaReflection();
    }

    public Map<String, String> requestHeaders() {

        return avroConverterConfig.requestHeaders();
    }

    public String basicAuthUserInfo() {
        return avroConverterConfig.basicAuthUserInfo();
    }
}
