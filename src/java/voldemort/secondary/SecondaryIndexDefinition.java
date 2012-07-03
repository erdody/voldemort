package voldemort.secondary;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Defines the required information for one secondary index field.
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 * <secondary-index>
 *     <name>status</name>
 *     <extractor-type>map</extractor-type>
 *     <extractor-info>status</extractor-info>
 *     <field-type>"int8"</schema-info>
 * </secondary-index>
 * }
 * </pre>
 */
public class SecondaryIndexDefinition {

    private final String name;
    private final String extractorType;
    private final String extractorInfo;
    private final String fieldType;

    public SecondaryIndexDefinition(String name,
                                    String extractorType,
                                    String extractorInfo,
                                    String fieldType) {
        this.name = name;
        this.extractorType = extractorType;
        this.extractorInfo = extractorInfo;
        this.fieldType = fieldType;
    }

    /**
     * Identifier for this secondary index field (i.e., how it will be
     * referenced by queries)
     */
    public String getName() {
        return name;
    }

    /**
     * Type of method used to extract information from the main value. e.g.
     * "map" for Map objects
     */
    public String getExtractorType() {
        return extractorType;
    }

    /**
     * Additional information about how to extract the secondary index field
     * information from the main value. e.g. for "map" extractor type, this
     * should contain the map key.
     */
    public String getExtractorInfo() {
        return extractorInfo;
    }

    /** Type of field */
    public String getFieldType() {
        return fieldType;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(name = " + this.name + ", extractor-type = "
               + extractorType + ", extractor-info = " + extractorInfo + ", field-type = "
               + fieldType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        else if(o == null)
            return false;
        else if(!(o.getClass() == SecondaryIndexDefinition.class))
            return false;

        SecondaryIndexDefinition def = (SecondaryIndexDefinition) o;
        return new EqualsBuilder().append(getName(), def.getName())
                                  .append(getExtractorType(), def.getExtractorType())
                                  .append(getExtractorInfo(), def.getExtractorInfo())
                                  .append(getFieldType(), def.getFieldType())
                                  .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 41).append(name)
                                          .append(extractorType)
                                          .append(extractorInfo)
                                          .append(fieldType)
                                          .toHashCode();
    }

}
