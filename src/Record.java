import java.nio.ByteBuffer;
import java.util.List;

/**
 * Represents a single record (row) in a table.
 * A record stores its data as a binary array for efficient storage and retrieval.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 * @contributor Tyler Black tcb8683
 */
public class Record {
    private byte[] data; // Binary representation of the record

    /**
     * Constructs a Record with the given binary data.
     * @param data The binary data representing the record.
     */
    public Record(byte[] data) {
        this.data = data;
    }

    /**
     * Computes the size of the record when stored in binary format.
     * @return The total size in bytes, including metadata.
     */
    public int computeSize() {
        return 4 + data.length; // 4 bytes for length prefix + actual data size
    }

    /**
     * Writes this record to a ByteBuffer for storage.
     * @param buffer The buffer to write to.
     */
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(data.length); // Store data length first
        buffer.put(data); // Store actual data
    }

    /**
     * returns the record object
     * @return the record object
     */
    public Record getRecords() {
        return this;
    }

    /**
     * gets the data from the record
     * @return the data from the record
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Helper function for getting the data of an attribute from a given buffer
     * @param attribute the attribute that the value being retrieved is
     * @param buffer buffer containing the data of this record
     * @param isNull byte of whether or not this attribute is null
     * @return the data retrived in byte form
     */
    private byte[] getAttributeData(AttributeSchema attribute, ByteBuffer buffer, byte isNull){
        byte[] returnData = null;
        if (isNull == 0){
            //boolean for checking if the current attribute is a VarChar
            boolean isVarChar = false; 
            if (attribute.getType() == AttributeType.VARCHAR){
                isVarChar = true;
            }

            //If attribute is a varchar, get attributeSize from 
            //  the next thing in buffer
            //Otherwise, attributeSize is gotten from attributeSchema
            int attributeSize = 0;
            if (isVarChar){
                attributeSize = buffer.getInt();
            } else {
                attributeSize = attribute.getSize();
            }

            //size of bytes to retrieve
            int dataSize = 0;
            if (attribute.getType() == AttributeType.CHAR ||
                attribute.getType() == AttributeType.VARCHAR){
                if (attribute.getType() == AttributeType.VARCHAR){
                    //add on 4 bytes for the size of the varChar
                    dataSize += 4;
                }
                dataSize += attributeSize*2;
            } else {
                dataSize += attributeSize;
            }

            returnData = new byte[dataSize];
            if(attribute.getType() == AttributeType.VARCHAR){
                //move the buffer back 4 bytes so the size of the varChar can be read in
                buffer.position(buffer.position() - 4);
            }
            for (int i = 0; i < dataSize; i++){
                returnData[i] = buffer.get();
            }
            
            return returnData;
        } else {
            returnData = new byte[0];
            return returnData;
        }
    }

    /**
     * Helper function for retrieving the value of an attribute from this record
     *  based on a list of attributes
     * @param attributeIndex the index of the attribute in the list to retrieve the value from
     * @param attributeSchemas a list of attributes of this record
     * @param buffer buffer containing the data of this record
     * @return the value of the attribute in byte form
     */
    private byte[] getValueForAttribute(int attributeIndex, List<AttributeSchema> attributeSchemas, ByteBuffer buffer){
        byte[] returnValue = null;
        int numAttributes = attributeSchemas.size();
        // move past size of record
        buffer.position(4);

        byte[] nullMap = new byte[numAttributes];
        // get the null map for record
        int byteIndex = 0;
        while (byteIndex < attributeIndex){
            nullMap[byteIndex] = buffer.get();
            byteIndex += 1;
        }
        //null byte of the attribute at attributeIndex
        byte isNull = buffer.get();
        if (isNull == 1){
            returnValue = new byte[0];
            return returnValue;
        } else {
            nullMap[byteIndex] = isNull;
            byteIndex += 1;
            while (byteIndex < numAttributes){
                nullMap[byteIndex] = buffer.get();
                byteIndex += 1;
            }
        }

        //skip through all the the attributes until arrive at the attribute at attribute index
        int currentAttributeIndex = 0;
        while (currentAttributeIndex < attributeIndex){
            AttributeSchema currentAttribute = attributeSchemas.get(currentAttributeIndex);
            getAttributeData(currentAttribute, buffer, nullMap[currentAttributeIndex]);
            currentAttributeIndex += 1;
        }
        returnValue = getAttributeData(attributeSchemas.get(currentAttributeIndex), buffer, nullMap[currentAttributeIndex]);

        return returnValue;
    }

    public Object getPrimaryKeyValue(TableSchema tableSchema){
        List<AttributeSchema> attributeSchemas = tableSchema.getAttributes();
        int primaryKeyIndex = 0;
        for (int i = 0; i < attributeSchemas.size(); i++){
            if (attributeSchemas.get(i).isPrimaryKey()){
                primaryKeyIndex = i;
                break;
            }
        }

        ByteBuffer bufferForOriginalData = ByteBuffer.allocate(computeSize());
        writeToBuffer(bufferForOriginalData);

        //use this to get the primary key as bytes
        byte[] primaryKeyAsBytes = getValueForAttribute(primaryKeyIndex, attributeSchemas, bufferForOriginalData);
        ByteBuffer primaryKeyBuffer = ByteBuffer.allocate(primaryKeyAsBytes.length);
        primaryKeyBuffer.put(primaryKeyAsBytes);
        primaryKeyBuffer.flip();
        AttributeSchema primaryKeyAttribute = attributeSchemas.get(primaryKeyIndex);

        Object primaryKey;
        AttributeType primaryKeyType = primaryKeyAttribute.getType();
        switch(primaryKeyType){
            case BOOLEAN:
                byte bool = primaryKeyBuffer.get();
                if (bool == 0){
                    primaryKey = false;
                }else{
                    primaryKey = true;
                }
                break;
            case INTEGER:
                primaryKey = primaryKeyBuffer.getInt();
                break;
            case DOUBLE:
                primaryKey = primaryKeyBuffer.getDouble();
                break;
            case CHAR:
                int charSize = primaryKeyAttribute.getSize();
                String primaryKeyString = "";
                for (int i = 0; i < charSize; i++){
                    primaryKeyString += primaryKeyBuffer.getChar();
                }
                primaryKey = primaryKeyString;
                break;
            case VARCHAR:
                int varCharSize = primaryKeyBuffer.getInt();
                primaryKeyString = "";
                for (int i = 0; i < varCharSize; i++){
                    primaryKeyString += primaryKeyBuffer.getChar();
                }
                primaryKey = primaryKeyString;
                break;
            default:
                primaryKey = null;
                break;
        }
        return primaryKey;
    }

    /**
     * Checks record to see if it contains the given value
     * @param tableSchema the schema of the table that this record belongs to
     * @param attributeIndex the index of the attribute in the list of attributes for this table that the value belongs to
     * @param value the value to search this record for
     * @return true if this record contains the value
     */
    public boolean checkForValue(TableSchema tableSchema, int attributeIndex, String value){
        List<AttributeSchema> attributeSchemas = tableSchema.getAttributes();

        byte[] valueAsBytes = valueToBytes(attributeSchemas.get(attributeIndex), value);

        ByteBuffer bufferForOriginalData = ByteBuffer.allocate(computeSize());
        writeToBuffer(bufferForOriginalData);

        //use this to get the original value so the size of it can be obtained
        byte[] originalValueAsBytes = getValueForAttribute(attributeIndex, attributeSchemas, bufferForOriginalData);

        if (valueAsBytes.length != originalValueAsBytes.length){
            return false;
        }

        for (int i = 0; i < valueAsBytes.length; i++){
            if (valueAsBytes[i] != originalValueAsBytes[i]){
                return false;
            }
        }
        return true;
    }

    /**
     * Removes the attribute from this record with the given name
     * @param attributeName the name of the attribute to remove from the record
     * @param tableSchema the old TableSchema of the table that this record belongs to
     * @reutrn the new record with the attribute removed
     */
    public Record removeAttribute(String attributeName, TableSchema tableSchema){
        List<AttributeSchema> attributeSchemas = tableSchema.getAttributes();

        //Find the index of the Attribute in the list of AttributeSchemas that is being removed
        int attributeIndex = 0;
        for (int i = 0; i < attributeSchemas.size(); i++){
            if (attributeSchemas.get(i).getName().equals(attributeName)){
                attributeIndex = i;
                break;
            }
        }

        byte[] newData = null;

        ByteBuffer buffer = ByteBuffer.allocate(computeSize());
        writeToBuffer(buffer);

        // move past size of record
        buffer.position(4);

        int oldNumAttributes = attributeSchemas.size();
        byte[] oldNullMap = new byte[oldNumAttributes];
        // get the null map for record
        for(int i = 0; i < oldNumAttributes; i++){
            oldNullMap[i] = buffer.get();
        }

        int newNumAttributes = oldNumAttributes - 1;
        byte[] newNullMap = new byte[newNumAttributes];
        int index = 0;
        for (int i=0; i<oldNumAttributes; i++){
            if (i != attributeIndex){
                newNullMap[index] = oldNullMap[i];
                index += 1;
            }
        }
        newData = newNullMap;

        index = 0; //index of current attribute
        while(index < oldNumAttributes){
            //if (index != attributeIndex){
            if (oldNullMap[index] == 0){
                AttributeSchema attributeSchema = attributeSchemas.get(index);
                //boolean for checking if the current attribute is a VarChar
                boolean isVarChar = false; 
                if (attributeSchema.getType() == AttributeType.VARCHAR){
                    isVarChar = true;
                }

                //If attribute is a varchar, get attributeSize from 
                //  the next thing in buffer
                //Otherwise, attributeSize is gotten from attributeSchema
                int attributeSize = 0;
                if (isVarChar){
                    attributeSize = buffer.getInt();
                } else {
                    attributeSize = attributeSchema.getSize();
                }

                if (index != attributeIndex){
                    //create a new array that is the size of 
                    //  already collected data + data being added
                    int newSize = newData.length;
                    if (attributeSchema.getType() == AttributeType.CHAR ||
                        attributeSchema.getType() == AttributeType.VARCHAR){
                        if (attributeSchema.getType() == AttributeType.VARCHAR){
                            //add on 4 bytes for the size of the varChar
                            newSize += 4;
                        }
                        newSize += attributeSize*2;
                    } else {
                        newSize += attributeSize;
                    }
                    byte[] addition = new byte[newSize];
                    if(attributeSchema.getType() == AttributeType.VARCHAR){
                        //move the buffer back 4 bytes so the size of the varChar can be read in
                        buffer.position(buffer.position() - 4);
                    }
                    for (int i = 0; i < newSize; i++){
                        if (i < newData.length){
                            addition[i] = newData[i];
                        }else{
                            addition[i] = buffer.get();
                        }
                    }
                    newData = addition;
                }else{
                    //Move through the buffer for attributeSize bytes
                    if (attributeSchema.getType() == AttributeType.CHAR ||
                        attributeSchema.getType() == AttributeType.VARCHAR){
                        for(int i = 0; i < attributeSize*2; i++){
                            buffer.get();
                        }
                    } else {
                        for(int i = 0; i < attributeSize; i++){
                            buffer.get();
                        }
                    }
                }
            }
            //}

            index += 1;
        }

        //return a new instance of a record with the new data
        return new Record(newData);
    }

    /**
     * converts the given string to bytes based on it's data
     *  type in the given attribute schema
     * @param attributeSchema the attribute schema that the given value is an attribute value of
     * @param value the string value to be converted to bytes
     * @return an array of bytes that represent the given value
     */
    private byte[] valueToBytes(AttributeSchema attributeSchema, String value){
        ByteBuffer buffer = null;
        if (value == null){
            buffer = ByteBuffer.allocate(0);
            return buffer.array();
        } else if (attributeSchema.getType() == AttributeType.VARCHAR){
            int lengthOfString = value.length();
            //4 bytes for the integer plus length of the string * 2 to 
            //  account for char being 2 bytes
            int lengthOfBuf = 4 + (lengthOfString*2);
            buffer = ByteBuffer.allocate(lengthOfBuf);
            buffer.putInt(lengthOfString);
            for (char character : value.toCharArray()){
                buffer.putChar(character);
            }
            return buffer.array();
        } else if (attributeSchema.getType() == AttributeType.CHAR){
            int lengthOfString = value.length();
            buffer = ByteBuffer.allocate(lengthOfString * 2);
            for (char character : value.toCharArray()){
                buffer.putChar(character);
            }
            return buffer.array();
        } else{
            buffer = ByteBuffer.allocate(attributeSchema.getSize());
            if (attributeSchema.getType() == AttributeType.BOOLEAN){
                byte[] src = new byte[1];
                if (value.equals("true")){
                    src[0] = 1;
                }else {
                    src[0] = 0;
                }
                buffer.put(src);
            } else if (attributeSchema.getType() == AttributeType.INTEGER){
                int intValue = Integer.valueOf(value);
                buffer.putInt(intValue);
            } else {
                double doubleValue = Double.valueOf(value);
                buffer.putDouble(doubleValue);
            }
            return buffer.array();
        }
    }

    /**
     * adds the given default value to the record as a new attribute value
     * @param newAttribute the attribute that is being add to the record
     * @param defaultValue the value that should be added to the record
     * @param oldTableSchema the table schema before adding the new attribute
     * @return the new record with the added attribute
     */
    public Record addAttribute(AttributeSchema newAttribute, String defaultValue, TableSchema oldTableSchema){
        List<AttributeSchema> attributeSchemas = oldTableSchema.getAttributes();

        byte[] valueAsBytes = valueToBytes(newAttribute, defaultValue);

        //new data size is the original size of the data + 
        //  1 byte for the new null bit map +
        //  the length of the value as bytes
        int newDataSize = data.length + 1 + valueAsBytes.length;
        ByteBuffer bufferForNewData = ByteBuffer.allocate(newDataSize);

        ByteBuffer bufferForOldData = ByteBuffer.allocate(computeSize());
        writeToBuffer(bufferForOldData);

        // move past size of record
        bufferForOldData.position(4);

        //get null map from old data and add to new data
        for(int i = 0; i < attributeSchemas.size(); i++){
            bufferForNewData.put(bufferForOldData.get());
        }
        //put new byte into the null map
        if (valueAsBytes.length == 0){
            bufferForNewData.put((byte)1);
        } else {
            bufferForNewData.put((byte)0);
        }

        //4 for the size of the record + 
        //  the number of attribute (number of bytes in the null map)
        int offsetPastByteMap = 4 + attributeSchemas.size();
        //gets all the rest of the original data and adds to new data
        for (int i = offsetPastByteMap; i < computeSize(); i++){
            bufferForNewData.put(bufferForOldData.get());
        }
        //put the new value into the buffer
        bufferForNewData.put(valueAsBytes);

        //return a new instance of a record with the new data
        byte[] newData = bufferForNewData.array();
        return new Record(newData);
    }

    /**
     * Changes the data of this record with the newData for an attribute
     * @param newData the new data to update the record with
     * @param attributeIndex the index in the list of table attributes of the attribute which's value should be changed
     * @param tableSchema the schema of the table that this record belongs to
     */
    public Record changeData(String newData, int attributeIndex, TableSchema tableSchema){
        List<AttributeSchema> attributeSchemas = tableSchema.getAttributes();
        int numAttributes = attributeSchemas.size();

        byte[] valueAsBytes = valueToBytes(attributeSchemas.get(attributeIndex), newData);

        ByteBuffer bufferForOriginalData = ByteBuffer.allocate(computeSize());
        writeToBuffer(bufferForOriginalData);

        //use this to get the original value so the size of it can be obtained
        byte[] originalValueAsBytes = getValueForAttribute(attributeIndex, attributeSchemas, bufferForOriginalData);

        //new data size is the original size of the data - 
        //  the size of the orignial attribute data +
        //  the size of the value as bytes
        int newDataSize = data.length - originalValueAsBytes.length + valueAsBytes.length;
        ByteBuffer bufferForNewData = ByteBuffer.allocate(newDataSize);

        bufferForOriginalData.position(0);
        // move past size of record
        bufferForOriginalData.position(4);

        byte[] newNullMap = new byte[numAttributes];
        // get the null map for record
        int byteIndex = 0;
        while (byteIndex < attributeIndex){
            newNullMap[byteIndex] = bufferForOriginalData.get();
            byteIndex += 1;
        }
        //skip over attribute null byte in original
        byte originalAttributeNullByte = bufferForOriginalData.get();
        //add new attribute null byte in new nullbit map
        if (newData == null){
            newNullMap[byteIndex] = 1;
        } else {
            newNullMap[byteIndex] = 0;
        }
        byteIndex += 1;
        while (byteIndex < numAttributes){
            newNullMap[byteIndex] = bufferForOriginalData.get();
            byteIndex += 1;
        }
        bufferForNewData.put(newNullMap);

        int currentAttributeIndex = 0;
        while (currentAttributeIndex < attributeIndex){
            AttributeSchema currentAttribute = attributeSchemas.get(currentAttributeIndex);
            byte[] attributeData = getAttributeData(currentAttribute, bufferForOriginalData, newNullMap[currentAttributeIndex]);
            bufferForNewData.put(attributeData);
            currentAttributeIndex += 1;
        }
        //skip over attribute data from original
        getAttributeData(attributeSchemas.get(currentAttributeIndex), bufferForOriginalData, originalAttributeNullByte);
        currentAttributeIndex += 1;
        //add new attribute data in new
        //don't need to check for null because if null valueAsBytes will be size 0
        bufferForNewData.put(valueAsBytes);
        while (currentAttributeIndex < numAttributes){
            AttributeSchema currentAttribute = attributeSchemas.get(currentAttributeIndex);
            byte[] attributeData = getAttributeData(currentAttribute, bufferForOriginalData, newNullMap[currentAttributeIndex]);
            bufferForNewData.put(attributeData);
            currentAttributeIndex += 1;
        }
        
        return new Record(bufferForNewData.array());
    }
}
