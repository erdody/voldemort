package voldemort.secondary;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import surver.pub.expression.FieldDefinition;
import surver.pub.expression.FieldType;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.serialization.json.JsonTypes;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/** Default factory for {@link SecondaryIndexProcessor} objects */
public class SecondaryIndexProcessorFactory {

    /**
     * Creates a new {@link SecondaryIndexProcessor} based on the secondary
     * index definitions and field type definition. Actual serializers are
     * created with the given serializerFactory.
     */
    public static SecondaryIndexProcessor getProcessor(SerializerFactory serializerFactory,
                                                       Collection<SecondaryIndexDefinition> secondaryIndexDefs,
                                                       SerializerDefinition valueSerializerDef) {
        Map<String, SecondaryIndexValueExtractor<?, ?>> secIdxExtractors = Maps.newHashMap();

        List<FieldDefinition> queryFields = Lists.newArrayList();
        Map<String, JsonTypes> jsonTypes = Maps.newLinkedHashMap();
        for(SecondaryIndexDefinition secDef: secondaryIndexDefs) {
            JsonTypes jsonType = JsonTypes.fromDisplay(secDef.getFieldType());
            jsonTypes.put(secDef.getName(), jsonType);
            secIdxExtractors.put(secDef.getName(), getExtractor(secDef));

            queryFields.add(new FieldDefinition(secDef.getName(), getFieldType(jsonType)));
        }

        Serializer<Object> secIdxSerializer = new JsonTypeSerializer(new JsonTypeDefinition(jsonTypes));

        return new DefaultSecondaryIndexProcessor(serializerFactory.getSerializer(valueSerializerDef),
                                                  secIdxSerializer,
                                                  secIdxExtractors,
                                                  queryFields);
    }

    private static FieldType getFieldType(JsonTypes jsonType) {
        switch(jsonType) {
            case INT8:
                return FieldType.INT8;
            case INT16:
                return FieldType.INT16;
            case INT32:
                return FieldType.INT32;
            case INT64:
                return FieldType.INT64;
            case BOOLEAN:
                return FieldType.BOOL;
            case STRING:
                return FieldType.STRING;
            case DATE:
                return FieldType.DATE;
            case BYTES:
            case FLOAT32:
            case FLOAT64:
                throw new IllegalArgumentException("Type not supported: " + jsonType);
        }
        throw new IllegalArgumentException("Type not supported: " + jsonType);
    }

    private static SecondaryIndexValueExtractor<?, ?> getExtractor(SecondaryIndexDefinition secDef) {
        String type = secDef.getExtractorType();
        if("map".equals(type)) {
            return new MapSecondaryIndexValueExtractor(secDef.getExtractorInfo());
        } else {
            throw new IllegalArgumentException("No known secondary index value extractor type: "
                                               + type);
        }
    }

}
