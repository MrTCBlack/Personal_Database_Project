import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a database page that stores multiple records.
 * A page is a fixed-size unit of storage in the database system.
 * Each page holds records and is stored as binary data.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 * @contributor Tyler Black tcb8683
 */
public class Page {
    private static int PAGE_SIZE; // The fixed size of the page
    private List<Record> records; // List of records stored in the page
    private int numRecords; // The current number of records in the page
    private int pageId;

    /**
     * Constructs an empty Page with the given page size.
     * @param pageSize The fixed size of the page in bytes.
     */
    public Page(int pageSize, int pageId) {
        PAGE_SIZE = pageSize;
        this.records = new ArrayList<>();
        this.numRecords = 0;
        this.pageId = pageId;
    }

    /**
     * Finds the primary key value in a record and returns it
     * @param record the record to find the primary key value in
     * @param attributeSchemas the list of attributeSchemas for the table that the record is in
     * @param primaryKeyAttributeIndex The index in the list of attributeSchemas that is the Attribute that is the primaryKey
     * @return a byte array of the primary key value
     */
    private byte[] getPrimaryKeyValue(Record record, List<AttributeSchema> attributeSchemas, int primaryKeyAttributeIndex){
        ByteBuffer buffer = ByteBuffer.allocate(record.computeSize());
        record.writeToBuffer(buffer);

        int numAttributes = attributeSchemas.size();
        byte[] nullMap = new byte[numAttributes];

        // move past size of record
        buffer.position(4);

        // get the null map for record
        for(int i = 0; i < numAttributes; i++){
            nullMap[i] = buffer.get();
        }

        int count = 0; //current attribute
        while (count < primaryKeyAttributeIndex){
            if (nullMap[count] == 0){
                AttributeSchema attributeSchema = attributeSchemas.get(count);
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
            count += 1;
        }

        AttributeSchema attributeSchema = attributeSchemas.get(count);
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

        byte[] primaryKeyValueByte = null;
        if (attributeSchema.getType() == AttributeType.CHAR ||
            attributeSchema.getType() == AttributeType.VARCHAR){
            primaryKeyValueByte = new byte[attributeSize*2];
        } else {
            primaryKeyValueByte = new byte[attributeSize];
        }
        //Move through the buffer for attributeSize bytes
        if (attributeSchema.getType() == AttributeType.CHAR ||
            attributeSchema.getType() == AttributeType.VARCHAR){
            for(int i = 0; i < attributeSize*2; i++){
                primaryKeyValueByte[i] = buffer.get();
            }
        } else {
            for(int i = 0; i < attributeSize; i++){
                primaryKeyValueByte[i] = buffer.get();
            }
        }
        return primaryKeyValueByte;
    }

    /**
     * Inserts a record to the end of the records array
     * This is mostly a helper function for when records are
     * being read back in from the file and being put in a new
     * Page object.
     * @param newRecord the record to insert into this page
     */
    public void insertAtTheEnd(Record newRecord){
        records.add(newRecord);
        numRecords += 1;
    }

    /**
     * Inserts a record at the given index in the record array
     * This is used for bPlusTree insertion
     * @param newRecord the record to insert into this page
     * @param index index to insert record at
     */
    public void insertAtIndex(Record newRecord, int index){
        records.add(index, newRecord);
        numRecords += 1;
    }

    /**
     * Adds a record to the page if there is enough space.
     * @param record The record to add.
     * @throws IllegalStateException If the page is full.
     */
    public boolean addRecord(Record newRecord, TableSchema tableSchema) {
            List<AttributeSchema> attributeSchemas = tableSchema.getAttributes();

            //Find the index of the Attribute in the list of AttributeSchemas that is the primarykey
            int primaryKeyAttributeIndex = 0;
            for (int i = 0; i < attributeSchemas.size(); i++){
                if (attributeSchemas.get(i).isPrimaryKey()){
                    primaryKeyAttributeIndex = i;
                    break;
                }
            }

            /*
             * Creates the new recordList after inserting the new record,
             *  if the new record should be inserted.
             * Gets the primary key value for the new record and loops through
             *  the current records, getting their primary key value and comparing
             *  it to the new record's primary key value.
             * If the primary key value of the new record is "less than" the primary
             *  key value of another record, the new record is inserted before the other
             *  record.
             */
            List<Record> newRecordsList = new ArrayList<>();
            byte[] newRecordPrimaryKeyValue = getPrimaryKeyValue(newRecord, attributeSchemas, primaryKeyAttributeIndex);
            int count = 0;
            boolean brokeOut = false;
            while (count < records.size()){
                byte[] recordPrimaryKeyValue = getPrimaryKeyValue(records.get(count), attributeSchemas, primaryKeyAttributeIndex);
                if (attributeSchemas.get(primaryKeyAttributeIndex).getType() == AttributeType.BOOLEAN){
                    boolean newRecordValue = false;
                    boolean recordValue = false;
                    if (newRecordPrimaryKeyValue[0] != 0){
                        newRecordValue = true;
                    }
                    if (recordPrimaryKeyValue[0] != 0){
                        recordValue = false;
                    }

                    if (newRecordValue == false && recordValue == true){
                        newRecordsList.add(newRecord);
                        newRecordsList.add(records.get(count));
                    }else{
                        newRecordsList.add(records.get(count));
                        newRecordsList.add(newRecord);
                    }
                    count += 1;
                    brokeOut = true;
                    break;
                } else if (attributeSchemas.get(primaryKeyAttributeIndex).getType() == AttributeType.INTEGER){
                    int newRecordValue = ByteBuffer.wrap(newRecordPrimaryKeyValue).getInt();

                    int recordValue = ByteBuffer.wrap(recordPrimaryKeyValue).getInt();
                    if (newRecordValue < recordValue){
                        newRecordsList.add(newRecord);
                        newRecordsList.add(records.get(count));
                        count += 1;
                        brokeOut = true;
                        break;
                    }
                    newRecordsList.add(records.get(count));
                } else if (attributeSchemas.get(primaryKeyAttributeIndex).getType() == AttributeType.DOUBLE){

                    double newRecordValue = ByteBuffer.wrap(newRecordPrimaryKeyValue).getDouble();

                    double recordValue = ByteBuffer.wrap(recordPrimaryKeyValue).getDouble();
                    if (newRecordValue < recordValue){
                        newRecordsList.add(newRecord);
                        newRecordsList.add(records.get(count));
                        count += 1;
                        brokeOut = true;
                        break;
                    }
                    newRecordsList.add(records.get(count));
                }else if (attributeSchemas.get(primaryKeyAttributeIndex).getType() == AttributeType.CHAR){
                    int charSize = attributeSchemas.get(primaryKeyAttributeIndex).getSize();

                    String newRecordValue = "";
                    ByteBuffer newRecordBuffer = ByteBuffer.wrap(newRecordPrimaryKeyValue);
                    for (int i = 0; i < charSize; i++){
                        newRecordValue += newRecordBuffer.getChar();
                    }

                    String recordValue = "";
                    ByteBuffer recordBuffer = ByteBuffer.wrap(recordPrimaryKeyValue);
                    for (int i = 0; i < charSize; i++){
                        recordValue += recordBuffer.getChar();
                    }
                    if (newRecordValue.compareTo(recordValue) < 0 ){
                        newRecordsList.add(newRecord);
                        newRecordsList.add(records.get(count));
                        count += 1;
                        brokeOut = true;
                        break;
                    }
                    newRecordsList.add(records.get(count));
                }else if (attributeSchemas.get(primaryKeyAttributeIndex).getType() == AttributeType.VARCHAR){
                    int newRecordValueSize = newRecordPrimaryKeyValue.length;
                    int recordValueSize = recordPrimaryKeyValue.length;

                    String newRecordValue = "";
                    ByteBuffer newRecordBuffer = ByteBuffer.wrap(newRecordPrimaryKeyValue);
                    for (int i = 0; i < (newRecordValueSize / 2); i++){
                        newRecordValue += newRecordBuffer.getChar();
                    }
                    
                    String recordValue = "";
                    ByteBuffer recordBuffer = ByteBuffer.wrap(recordPrimaryKeyValue);
                    for (int i = 0; i < (recordValueSize / 2); i++){
                        recordValue += recordBuffer.getChar();
                    }
                    if (newRecordValue.compareTo(recordValue) < 0 ){
                        newRecordsList.add(newRecord);
                        newRecordsList.add(records.get(count));
                        count += 1;
                        brokeOut = true;
                        break;
                    }
                    newRecordsList.add(records.get(count));
                }
                count += 1;
            }
            while (count < records.size()){
                newRecordsList.add(records.get(count));
                count+=1;
            }

            /*
             * brokeOut will be true if the new record was added.
             * If so, the new record list will replace the old one,
             *  the number of records is increased, and true is returned.
             * Otherwise, false is returned, so that the record can be
             *  attempted to be added to the next page
             */
            if(!brokeOut){
                return false;
            }else{
                records = newRecordsList;
                numRecords += 1;
                return true;
            }
    }

    /**
     * Computes the total size of the page, including stored records.
     * @return The size of the page in bytes.
     */
    public int computeSize() {
        int size = 4; // 4 bytes for numRecords metadata
        for (Record record : records) {
            size += record.computeSize();
        }
        return size;
    }

    /**
     * Sets the global page size. This should be called before creating pages.
     * @param pageSize The fixed size of pages in the system.
     */
    public static void setPageSize(int pageSize) {
        PAGE_SIZE = pageSize;
    }

    /**
     * get the page object
     * @return the page object
     */
    public Page getPage() {
        return this;
    }

    /**
     * get all records stored in page
     * @return all records stored in page
     */
    public List<Record> getRecords() {
        return records;
    }

    /**
     * check if page is full
     * @return if the page is full
     */
    public boolean isFull() {
        return computeSize() >= PAGE_SIZE;
    }

    /**
     * get the number of records stored in page
     * @return the number of records stored in page
     */
    public int getNumRecords(){
        return numRecords;
    }

    /**
     * computes if the size of the pages area is larger than
     *      the max pageSize
     * @return
     */
    public boolean pageIsGreaterThanPageSize(){
        int totalSize = this.computeSize();
        if (totalSize >= PAGE_SIZE){
            return true;
        }
        return false;
    }

    /**
     * Sets the given list of records to this Page's list of records.
     * Also changes the size of numRecords accordingly
     * @param newRecords the Record list to set as this Page's
     */
    public void setRecords(List<Record> newRecords){
        this.records = newRecords;
        numRecords = newRecords.size();
    }

    /**
     * Removes a record from this page at the given index
     * @param recordIndex the index of the record to remove in this page
     */
    public void removeRecord(int recordIndex){
        records.remove(recordIndex);
        numRecords -= 1;
    }

    /**
     * returns of the id of this page
     * @return the id of this page
     */
    public int getPageId(){
        return pageId;
    }

}
