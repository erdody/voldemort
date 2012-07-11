package voldemort.secondary;

import java.util.List;
import java.util.Map;

import surver.pub.expression.FieldDefinition;

/**
 * Class responsible for extracting and serializing secondary field values.
 */
public interface SecondaryIndexProcessor {

    /**
     * Based on the main value, extract all secondary field values
     * 
     * @return block with serialized secondary values
     */
    byte[] extractSecondaryValues(byte[] value);

    /**
     * Convert serialized secondary values to their respective Object
     * representation
     * 
     * @return secondary values, indexed by field name
     */
    Map<String, Object> parseSecondaryValues(byte[] values);

    /** @return List of fields available for secondary queries */
    List<FieldDefinition> getSecondaryFields();

}