package voldemort.secondary;

import java.util.List;
import java.util.Map;

import surver.pub.expression.FieldDefinition;

/**
 * Class responsible for extracting and serializing secondary field values
 */
public interface SecondaryIndexProcessor {

    /** Based on the main value, extract all secondary field values */
    byte[] extractSecondaryValues(byte[] value);

    /** @return list of secondary field names */
    List<String> getSecondaryFields();

    Map<String, Object> parseSecValues(byte[] values);

    List<FieldDefinition> getQueryFieldDefinitions();

}