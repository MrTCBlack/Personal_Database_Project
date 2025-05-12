import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the schema for a table, storing its attributes and metadata.
 * This schema is stored separately from the actual table data and is read from/written to binary files.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 */
public class TableSchema {
    private final String tableName; // The name of the table
    private final int tableNum; // Unique identifier for the table
    private final List<AttributeSchema> attributes; // List of attributes defining the schema
    private int N;

    /**
     * Constructs a TableSchema with the given table name and unique identifier.
     * @param tableName The name of the table.
     * @param tableNum The unique identifier of the table.
     */
    public TableSchema(String tableName, int tableNum) {
        this.tableName = tableName;
        this.tableNum = tableNum;
        this.attributes = new ArrayList<>();
        this.N = 0;
    }

    /**
     * Adds a new attribute to the table schema.
     * @param attribute The AttributeSchema object defining the attribute.
     */
    public void addAttribute(AttributeSchema attribute) {
        attributes.add(attribute);
    }

    /**
     * removes a column from the table schema
     * @param name the name of the column to be deleted
     */
    public void removeColumn(String name){

        for(AttributeSchema attr : attributes){

            if(attr.getName().equals(name)){

                attributes.remove(attr);
                break;
            }
        }
    }

    /**
     * Gets the name of the table associated with this schema.
     * @return The table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets the unique identifier of the table associated with this schema.
     * @return The table number.
     */
    public int getTableNum() {
        return tableNum;
    }

    /**
     * Gets the list of attributes defining the table schema.
     * @return A list of AttributeSchema objects.
     */
    public List<AttributeSchema> getAttributes() {
        return attributes;
    }

    /**
     * Reads a TableSchema from a binary buffer.
     * @param buffer The ByteBuffer containing the schema data.
     * @param tableName The name of the table.
     * @param tableNum The unique identifier of the table.
     * @return A TableSchema object with the extracted information.
     */
    public static TableSchema readFromBuffer(ByteBuffer buffer, String tableName, int tableNum) {
        int attributeCount = buffer.getInt();
        TableSchema schema = new TableSchema(tableName, tableNum);

        for (int i = 0; i < attributeCount; i++) {
            schema.addAttribute(AttributeSchema.readFromBuffer(buffer));
        }
        return schema;
    }

    /**
     * Writes the TableSchema to a binary buffer for storage.
     * @param buffer The ByteBuffer to write to.
     */
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(attributes.size()); // Store the number of attributes
        for (AttributeSchema attribute : attributes) {
            attribute.writeToBuffer(buffer); // Store each attribute
        }
    }

    public void computeBPlusTreeN(int pageSize) {
        AttributeSchema pk = getPrimaryKey();
        int keySize;

        switch (pk.getType()) {
            case BOOLEAN -> keySize = 1;
            case INTEGER -> keySize = 4;
            case DOUBLE -> keySize = 8;
            case CHAR, VARCHAR -> keySize = pk.getSize() * 2;
            default -> throw new IllegalArgumentException("Unsupported key type");
        }

        int pairSize = keySize + 8; // pointer = 2 * int (page + index)
        this.N = Math.floorDiv(pageSize, pairSize) - 1;
    }

    public int getN(){
        return this.N;
    }

    public AttributeSchema getPrimaryKey() {
        for (AttributeSchema attr : attributes) {
            if (attr.isPrimaryKey()) return attr;
        }
        throw new IllegalStateException("No primary key defined");
    }

}
