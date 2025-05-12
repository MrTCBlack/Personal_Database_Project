/**
 * Represents an attribute (column) in a database schema.
 * Stores details such as the attribute name, type, size (if applicable),
 * and constraints (like primary key, uniqueness, and nullability).
 * The schema is used to define the structure of tables in the database.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 */
import java.nio.ByteBuffer;

/**
 * The AttributeSchema class defines an individual column of a database table.
 * It contains metadata about the column's data type, constraints, and size.
 */
public class AttributeSchema {
    /** The name of the attribute (column). */
    private String name;
    /** The data type of the attribute (INTEGER, DOUBLE, BOOLEAN, CHAR, VARCHAR). */
    private AttributeType type;
    /** The size of the attribute, applicable for CHAR(N) and VARCHAR(N). */
    private int size; // Only for CHAR(N) and VARCHAR(N)
    /** Indicates whether this attribute is the primary key for the table. */
    private boolean isPrimaryKey;
    /** Indicates whether this attribute must have unique values. */
    private boolean isUnique;
    /** Indicates whether this attribute can accept null values. */
    private boolean isNotNull;

    /**
     * Constructs an AttributeSchema with the given properties.
     * @param name The name of the attribute.
     * @param type The data type of the attribute.
     * @param size The size of the attribute (for CHAR and VARCHAR types).
     * @param isPrimaryKey True if this attribute is the primary key.
     * @param isUnique True if this attribute must be unique.
     * @param isNotNull True if this attribute cannot be null.
     */
    public AttributeSchema(String name, AttributeType type, int size, boolean isPrimaryKey, boolean isUnique, boolean isNotNull) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.isPrimaryKey = isPrimaryKey;
        this.isUnique = isUnique;
        this.isNotNull = isNotNull;
    }

    /**
     * gets the name of the attribute
     * 
     * @return name of the attritibute
     */
    public String getName() {
        return name;
    }

    /**
     * gets the type of the attribute
     * @return the type of the attribute
     */
    public AttributeType getType() {
        return type;
    }

    /**
     * gets the length of the attribute, used for char and varchar;
     * if char, its exact length; if varchar, its max length
     * @return the length of the attribute
     */
    public int getSize() {
        return size;
    }

    /**
     * returns ifthe  attribute is a primary key
     * @return if the attribute is a primary key
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     * returns if the attribute has a unique constraint
     * @return if the attribute has a unique constraint
     */
    public boolean isUnique() {
        return isUnique;
    }

    /**
     * returns if the attribute has a not null constraint
     * @return if the attribute has a not null constraint
     */
    public boolean isNotNull() {
        return isNotNull;
    }

    /**
     * Writes this attribute's metadata to a binary buffer.
     * This method is used to persist the schema in a binary format.
     * @param buffer The ByteBuffer where the data will be written.
     */
    public void writeToBuffer(ByteBuffer buffer) {
        byte[] nameBytes = name.getBytes();
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);
        buffer.putInt(type.ordinal());
        buffer.putInt(size);
        
        // Encode constraints as bitmask (bit 0 = PrimaryKey, bit 1 = Unique, bit 2 = NotNull)
        int constraints = (isPrimaryKey ? 1 : 0) | (isUnique ? 2 : 0) | (isNotNull ? 4 : 0);
        buffer.putInt(constraints);
    }

    /**
     * Reads an AttributeSchema object from a binary buffer.
     * Used when loading table schemas from persistent storage.
     * @param buffer The ByteBuffer containing attribute schema data.
     * @return A reconstructed AttributeSchema object.
     */
    public static AttributeSchema readFromBuffer(ByteBuffer buffer) {
        int nameLength = buffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        String name = new String(nameBytes);

        AttributeType type = AttributeType.values()[buffer.getInt()];
        int size = buffer.getInt();

        int constraints = buffer.getInt();
        boolean isPrimaryKey = (constraints & 1) != 0;
        boolean isUnique = (constraints & 2) != 0;
        boolean isNotNull = (constraints & 4) != 0;

        return new AttributeSchema(name, type, size, isPrimaryKey, isUnique, isNotNull);
    }
}
