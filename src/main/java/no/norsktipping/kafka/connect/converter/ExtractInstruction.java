package no.norsktipping.kafka.connect.converter;

import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.function.Function;

public class ExtractInstruction {
    private final Function<Object, SchemaAndValue> converterFunction;

    public Function<Object, SchemaAndValue> getConverterFunction() {
        return converterFunction;
    }

    public ExtractInstruction(Function<Object, SchemaAndValue> converterFunction) {
        this.converterFunction = converterFunction;
    }
}
