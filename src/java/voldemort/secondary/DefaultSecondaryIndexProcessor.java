package voldemort.secondary;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import surver.pub.expression.FieldDefinition;
import voldemort.serialization.Serializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Default SecondaryIndexProcessor implementation, that uses
 * {@link SecondaryIndexValueExtractor} objects to retrieve secondary values and
 * Serializer objects to convert them into byte arrays.
 */
public class DefaultSecondaryIndexProcessor implements SecondaryIndexProcessor {

    private final Serializer<?> valueSerializer;
    private final Serializer<Object> secIdxSerializer;
    private final Map<String, SecondaryIndexValueExtractor<?, ?>> secIdxExtractors;
    private final List<FieldDefinition> queryFieldDefinitions;

    /**
     * New secondary index processor.
     * 
     * @param valueSerializer how to transform primary byte arrays from/to an
     *        object
     * @param secIdxSerializer secondary field serializer
     * @param secIdxExtractors secondary field value extractors (by secondary
     *        field name).
     */
    public DefaultSecondaryIndexProcessor(Serializer<?> valueSerializer,
                                          Serializer<Object> secIdxSerializer,
                                          Map<String, SecondaryIndexValueExtractor<?, ?>> secIdxExtractors,
                                          List<FieldDefinition> queryFieldDefinitions) {
        this.valueSerializer = valueSerializer;
        this.secIdxSerializer = secIdxSerializer;
        this.secIdxExtractors = secIdxExtractors;
        this.queryFieldDefinitions = queryFieldDefinitions;
    }

    public byte[] extractSecondaryValues(byte[] serializedValue) {
        Object obj = valueSerializer.toObject(serializedValue);

        Map<String, Object> values = Maps.newHashMap();
        for(Entry<String, SecondaryIndexValueExtractor<?, ?>> entry: secIdxExtractors.entrySet()) {
            String fieldName = entry.getKey();
            values.put(fieldName, extract(entry.getValue(), obj));
        }
        return secIdxSerializer.toBytes(values);
    }

    @SuppressWarnings("unchecked")
    private Object extract(SecondaryIndexValueExtractor<?, ?> extractor, Object obj) {
        return ((SecondaryIndexValueExtractor<Object, Object>) extractor).extractValue(obj);
    }

    public List<String> getSecondaryFields() {
        return Lists.newArrayList(secIdxExtractors.keySet());
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> parseSecValues(byte[] values) {
        return (Map<String, Object>) secIdxSerializer.toObject(values);
    }

    public List<FieldDefinition> getQueryFieldDefinitions() {
        return queryFieldDefinitions;
    }

}