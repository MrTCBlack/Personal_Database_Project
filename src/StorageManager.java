
// Tells the pagebuffer what to do
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Represents a database storage manager that handles record storage and retrieval.
 * This class interacts with the database file system to manage records and pages.
 * StorageManager is responsible for reading and writing records to disk in a binary format.
 * 
 * @authors Aum Patel, akp9018@rit.edu
 * @Contributor Tyler Black, tcb8683
 */
public class StorageManager {
    private static String dbLocation;
    private static int pageSize;
    private static int bufferSize;
    private static PageBuffer pageBuffer;
    private static Catalog catalog;
    
    /**
     * Constructor for the Storage Manager
     * @param dbLocation the path to the location of the database directory
     * @param pageSize the size of a page
     * @param bufferSize the size of the bugger
     * @param catalog the catalog for the database
     */
    public StorageManager(String dbLocation, int pageSize, int bufferSize, Catalog catalog) {
        this.dbLocation = dbLocation;
        this.pageSize = pageSize;
        this.bufferSize = bufferSize;
        this.pageBuffer = new PageBuffer(bufferSize, dbLocation, pageSize);
        this.catalog = catalog;
    }
    
    /**
     * If Catalog can not find location of catalog file, it calls
     *      this function to create a new catalog file
     * @param catalogFile File for the catalog
     * @param pageSize Size of all pages for the database
     * @throws IOException
     */
    public static void createNewCatalogFile(File catalogFile, int pageSize, boolean indexOn) throws IOException{
        catalogFile.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(catalogFile, "rw");
        // Guaranteed empty file since we just created it
        // Load the file with the pageSize and bufferSize (loaded from main) and pass in 0 for
        // current number of tables
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.putInt(pageSize); //pageSize
        buffer.putInt(0);   //starting number of tables
        if (indexOn){
            buffer.put((byte)1);
        }else{
            buffer.put((byte)0);
        }
        raf.write(buffer.array());
        raf.close();
        return;
    }

    /**
     * Loads the Catalog from the Catalog File and creates
     *      the TableSchemas for all the tables to be stored in
     *      the Catalog
     * Also returns the old pageSize that was already in the Catalog
     *      to replace the new one provided in the command line arguments
     * @param tableSchemasByNum Map of TableID to TableSchema to be stored in Catalog
     * @param tableSchemasByName Map of TableName to TableSchema to be stored in Catalog
     * @param catalogFile The File where the Catalog is stored
     * @return oldPageSize - the page size that was read from the Catalog
     * @throws IOException
     */
    public static int loadCatalogFromFile(Map<Integer, TableSchema> tableSchemasByNum, Map<String, TableSchema> tableSchemasByName, 
                                            Map<Integer, Integer> treeNodes, Map<Integer, Integer> treeNumPages, 
                                            Map <Integer, List<Integer>> tablePageOrder, File catalogFile) throws IOException{
        try (RandomAccessFile raf = new RandomAccessFile(catalogFile, "r");
            FileChannel channel = raf.getChannel()) {
            //TODO: Fix this size problem
            ByteBuffer buffer = ByteBuffer.allocate((int) catalogFile.length());
            channel.read(buffer);   //read the catalog into a buffer
            buffer.flip();  //flip so that buffer position is 0

            //reads in page size for database and returns it to
            //  change page size in catalog
            int oldPageSize = buffer.getInt();
            int numTables = buffer.getInt(); //reads in the number of tables
            byte indexOn = buffer.get();
            if(indexOn == 1){
                Catalog.setIndex(true);
            }else{
                Catalog.setIndex(false);
            }

            if (Catalog.isIndexOn()){
                //Read in BplusTreeNode root from catalog
                for (int i = 0; i < numTables; i++){
                    int tableId = buffer.getInt();
                    int nodePointer = buffer.getInt();
                    treeNodes.put(tableId, nodePointer);
                }
            }

            //Read in number of pages for tables from catalog
            /*for (int i = 0; i < numTables; i++){
                int tableId = buffer.getInt();
                int numPages = buffer.getInt();
                tableNumPages.put(tableId, numPages);
            }*/

            if (Catalog.isIndexOn()){
                //Read in number of pages for BplusTrees from catalog
                for (int i = 0; i < numTables; i++){
                    int tableId = buffer.getInt();
                    int numPages = buffer.getInt();
                    treeNumPages.put(tableId, numPages);
                }
            }

            //Read in number of pages for tables from catalog
            for (int i = 0; i < numTables; i++){
                int tableId = buffer.getInt();
                int numPages = buffer.getInt();
                for (int indx = 0; indx < numPages; indx++){
                    Catalog.addPageAtIndex(tableId, indx, buffer.getInt());
                }
            }
        
            //Read in table name and table schema from catalog
            for (int i = 0; i < numTables; i++) {
                int tableNum = buffer.getInt();
                int tableNameLength = buffer.getInt();
                byte[] tableNameBytes = new byte[tableNameLength];
                buffer.get(tableNameBytes);
                String tableName = new String(tableNameBytes);

                TableSchema schema = readTableSchema(buffer, tableName, tableNum, oldPageSize);
                tableSchemasByNum.put(tableNum, schema);
                tableSchemasByName.put(tableName, schema);
            }
            raf.close();
            return oldPageSize;
        }
    }

    /**
     * Reads a TableSchema from a binary buffer.
     * @param buffer The ByteBuffer containing the schema data.
     * @param tableName The name of the table.
     * @param tableNum The unique identifier of the table.
     * @return A TableSchema object with the extracted information.
     */
    private static TableSchema readTableSchema(ByteBuffer buffer, String tableName, int tableNum, int pageSize) {
        int attributeCount = buffer.getInt();
        TableSchema schema = new TableSchema(tableName, tableNum);

        for (int i = 0; i < attributeCount; i++) {
            schema.addAttribute(readAttributeFromBuffer(buffer));
        }

        if (catalog.isIndexOn()){
            schema.computeBPlusTreeN(pageSize);
        }
        return schema;
    }

    /**
     * Reads an AttributeSchema object from a binary buffer.
     * Used when loading table schemas from persistent storage.
     * @param buffer The ByteBuffer containing attribute schema data.
     * @return A reconstructed AttributeSchema object.
     */
    private static AttributeSchema readAttributeFromBuffer(ByteBuffer buffer) {
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

    /**
     * Saves the catalog to disk
     * @param catalogFile the location of the catalog file
     * @param pageSize the size of a Page
     * @param tableSchemasByNum a Map of tableId to TableSchema
     * @param tableSchemasByName a Map of TableName to TableSchema
     * @throws IOException
     */
    //TODO: COME BACK AND REWORK SO THAT IT TAKES UP LESS SPACE
    public static void saveCatalog(String catalogFile, int pageSize, 
                                    Map<Integer, TableSchema> tableSchemasByNum, Map<String, TableSchema> tableSchemasByName,
                                    Map<Integer, Integer> treeNodes, Map<Integer, Integer> treeNumPages, 
                                    Map<Integer, List<Integer>> tablePageOrder, boolean indexOn) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(catalogFile, "rw");
             FileChannel channel = raf.getChannel()) {
            //TODO: CHANGE THIS AND ALL OTHERS SO THAT IT ISN"T HARD CODED
            ByteBuffer buffer = ByteBuffer.allocate(2048);
            buffer.putInt(pageSize);
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
            buffer.putInt(tableSchemasByNum.size());
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
            if (indexOn){
                buffer.put((byte) 1);
            } else {
                buffer.put((byte) 0);
            }
            buffer.flip();
            channel.write(buffer);
            buffer.clear();

            if (indexOn){
                //Put BplusTreeNode root into catalog
                for (Map.Entry<Integer, Integer> entry : treeNodes.entrySet()){
                    int tableId = entry.getKey();
                    int rootPointer = entry.getValue();
                    buffer.putInt(tableId);
                    buffer.putInt(rootPointer);
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }
            }

            if (indexOn){
                //Put BplusTree number of pages into catalog
                for (Map.Entry<Integer, Integer> entry: treeNumPages.entrySet()){
                    int treeId = entry.getKey();
                    int numPages = entry.getValue();
                    buffer.putInt(treeId);
                    buffer.putInt(numPages);
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }
            }

            //Put table page order into catalog
            for (Map.Entry<Integer, List<Integer>> entry: tablePageOrder.entrySet()){
                int treeId = entry.getKey();
                int numPages = entry.getValue().size();
                buffer.putInt(treeId);
                buffer.putInt(numPages);
                for (Integer pageId: entry.getValue()){
                    buffer.putInt(pageId);
                }
                buffer.flip();
                channel.write(buffer);
                buffer.clear();
            }

            //Put Table name and it's schema into the catalog
            for (Map.Entry<Integer, TableSchema> entry : tableSchemasByNum.entrySet()) {
                String tableName = entry.getValue().getTableName();
                byte[] nameBytes = tableName.getBytes();
                buffer.putInt(tableSchemasByName.get(tableName).getTableNum());
                buffer.putInt(nameBytes.length);
                buffer.put(nameBytes);
                entry.getValue().writeToBuffer(buffer);
                buffer.flip();
                channel.write(buffer);
                buffer.clear();
            }

            //buffer.flip();
            //channel.write(buffer);
            raf.close();
        }
    }

    /**
     * Rewrites the table header of a table file
     * @param tableId the id of the table file that will be rewritten
     * @param indexToEnter the index in the page order to enter the new page id
     * @param newPageId the new page id to enter
     * @return updated order of page id
     * @throws IOException
     */
    /*public static List<Integer> rewriteTableFileHeader(int tableId, int indexToEnter, int newPageId, boolean getTreeOrder) throws IOException{
        String filePath;
        if (getTreeOrder){
            filePath = dbLocation + "/indexes/tree" + tableId + ".bpt";
        } else {
            filePath = dbLocation + "/tables/table" + tableId + ".tbl";
        }
        //String filePath = dbLocation + "/tables/table" + tableId + ".tbl";
        File tableFile = new File(filePath);
        if (!tableFile.exists()){
            System.out.println("table"+tableId+" does not exist.");
            return null;
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "rw"); FileChannel channel = raf.getChannel()) {

            //get the order of the pages

            List<Integer> pageOrder = getPageOrder(tableId, getTreeOrder);
            int numPages = pageOrder.size();

            //Based on the indexToEnter the new page in, make the new pageOrder
            List<Integer> newPageOrder = new ArrayList<>();
            int count = 0;            while (count < indexToEnter){
                newPageOrder.add(pageOrder.get(count));
                count += 1;
            }
            newPageOrder.add(newPageId);
            while (count < pageOrder.size()){
                newPageOrder.add(pageOrder.get(count));
                count += 1;
            }
            
            AttributeSchema primaryKey = null;
            for (AttributeSchema as : catalog.getTableSchemaByNum(tableId).getAttributes()){
                if (as.isPrimaryKey()){
                    primaryKey = as;
                    break;
                }
            }
            //perform shifting of pages
            pageBuffer.flush(0, getTreeOrder);
            count = newPageOrder.size() - 1;
            while (count > indexToEnter){
                //shift all pages right pageSize+4 to account for added page and pageid
                pageBuffer.shiftPage(tableId, newPageOrder.get(count), pageOrder, pageSize+4, primaryKey, getTreeOrder);
                count -= 1;
            }
            count -= 1;
            pageBuffer.flush(pageSize+4, getTreeOrder);
            while (count > -1){
                //shift all pages right 4 to account for added pageid
                pageBuffer.shiftPage(tableId, newPageOrder.get(count), pageOrder, 4, primaryKey, getTreeOrder);
                count -= 1;
            }

            numPages += 1;
            channel.position(0); //will write to the beginning of the file

            ByteBuffer numPagesBuffer = ByteBuffer.allocate(4);
            numPagesBuffer.putInt(numPages); //put in the buffer the new number of pages
            numPagesBuffer.flip();
            channel.write(numPagesBuffer);

            //put in buffer the new page order
            int totalPages = newPageOrder.size();
            ByteBuffer writeBuffer = null;
            if (totalPages * 4 > pageSize){
                writeBuffer = ByteBuffer.allocate(pageSize);
            } else {
                writeBuffer = ByteBuffer.allocate(totalPages *4);
            }
            for (int i = 0; i < newPageOrder.size(); i++){
                if (writeBuffer.position() + 4 > pageSize){
                    writeBuffer.flip(); //flip buffer. buffer position 0
                    channel.write(writeBuffer); //write the data that is in the buffer
                    if (totalPages * 4 > pageSize){
                        writeBuffer = ByteBuffer.allocate(pageSize);
                    } else {
                        writeBuffer = ByteBuffer.allocate(totalPages *4);
                    }
                }

                writeBuffer.putInt(newPageOrder.get(i));
                totalPages -= 1;
                  //writing the pageOrder
            }
            writeBuffer.flip(); //flip buffer. buffer position 0

            channel.write(writeBuffer); //write the data that is in the buffer

            return newPageOrder;
        }

    }*/
    
    /**
     * Gets a record given an table id and a primaryKeyValue
     * @param tableId the id of the table to retrieve the record from
     * @param primaryKeyValue the primary key value of the record
     * @return the retrieved record with the given primary key value
     * @throws IOException
     */
    public Record getRecord(int tableId, String primaryKeyValue) throws IOException {
        TableSchema tableSchema = catalog.getTableSchemaByNum(tableId); 
        List<AttributeSchema> attributeSchemas = tableSchema.getAttributes(); 
        
        int primaryKeyIndex = 0;

        // Finds the primary Key Index
        AttributeSchema primaryKeyAttributeSchema = null;
        for (int i = 0; i < attributeSchemas.size(); i++){
            if (attributeSchemas.get(i).isPrimaryKey()){
                primaryKeyAttributeSchema = attributeSchemas.get(i);
                primaryKeyIndex = i;
                break;
            }
        }

        //List<Integer> pageOrder = getPageOrder(tableId, false);
        //List<Integer> pageOrder = Catalog.getTablePageOrder(tableId); 

        // Loops through the pageOrder to get the page and then the records
        //for(int k = 0; k < pageOrder.size(); k++){
        for (Integer pageID: Catalog.getTablePageOrder(tableId)){
            Page page = (Page)pageBuffer.getPage(tableId, pageID, primaryKeyAttributeSchema, false);
            //List<Record> records = page.getRecords();
            
            // Search for the record with the given primary key
            //for (int i = 0; i < records.size(); i++){
            for (Record curRecord: page.getRecords()){  
                //Record curRecord = records.get(i); // Current record
                byte[] primaryKeyByte = getPrimaryKeyValue(curRecord, attributeSchemas, primaryKeyIndex);
            
                //Check if the primary key is a boolean
                if (attributeSchemas.get(primaryKeyIndex).getType() == AttributeType.BOOLEAN){
                    String strBool = "";
                    if(primaryKeyByte[0] == 0){
                        strBool = "false";   
                    }
                    else{
                    strBool = "true";
                    }
                    if (strBool.equals(primaryKeyValue)){
                        return curRecord;
                    }
                }

                //Check if the primary key is an integer
                else if (attributeSchemas.get(primaryKeyIndex).getType() == AttributeType.INTEGER){
                    String strInt = "";
                    int recordValue = ByteBuffer.wrap(primaryKeyByte).getInt();
                    strInt = Integer.toString(recordValue);
                    if (strInt.equals(primaryKeyValue)){
                        return curRecord;
                    }
                }

                //Check if the primary key is a double
                else if (attributeSchemas.get(primaryKeyIndex).getType() == AttributeType.DOUBLE){
                    String strDouble = "";
                    double recordValue = ByteBuffer.wrap(primaryKeyByte).getDouble();
                    strDouble = Double.toString(recordValue);
                    if (strDouble.equals(primaryKeyValue)){
                        return curRecord;
                    }
                }

                //Check if the primary key is a char
                else if (attributeSchemas.get(primaryKeyIndex).getType() == AttributeType.CHAR){
                    int charSize = attributeSchemas.get(primaryKeyIndex).getSize();
                    String strChar = "";
                    ByteBuffer buffer = ByteBuffer.wrap(primaryKeyByte);
                    for (int j = 0; j < charSize; j++){
                        strChar += buffer.getChar();
                    }
                    if (strChar.equals(primaryKeyValue)){
                        return curRecord;
                    }
                }

                //Check if the primary key is a varchar
                else if (attributeSchemas.get(primaryKeyIndex).getType() == AttributeType.VARCHAR){
                    int varcharSize = primaryKeyByte.length;;
                    String strVarChar = "";
                    ByteBuffer buffer = ByteBuffer.wrap(primaryKeyByte);
                    for (int j = 0; j < (varcharSize / 2); j++){
                        strVarChar += buffer.getChar();
                        }
                    if (strVarChar.equals(primaryKeyValue)){
                        return curRecord;
                    }
                }
            }
        }
        return null;
    }
 
    /**
     * Helper function to get the primary key value from a record
     * @param record Record to retrieve primary key value from
     * @param attributeSchemas the attribute schema associated with the table that the record is from
     * @param primaryKeyIndex the index in the attribute schema that holds the primary key
     * @return the byte array representing the primary key value
     */
    private byte[] getPrimaryKeyValue(Record record, List<AttributeSchema> attributeSchemas, int primaryKeyIndex){
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
        while (count < primaryKeyIndex){
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
     * Gets the order of pages in the table with the given tableId
     * @param tableId the id of the table to get the page order from
     * @return a list of page ids
     * @throws IOException
     */
    /*public static List<Integer> getPageOrder(int tableId, boolean getTreeOrder) throws IOException{
        String filePath = "";
        if(getTreeOrder){
            filePath = dbLocation + "/indexes/tree" + tableId + ".bpt";
        }else{
            filePath = dbLocation + "tables/table" + tableId + ".tbl";
        }
        File tableFile = new File(filePath);
        if (!tableFile.exists()){
            System.out.println("table"+tableId+" does not exist.");
            return null;
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r"); FileChannel channel = raf.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);
            channel.read(buffer); //reads pageSize bytes into buffer from start of file
            buffer.flip(); //flip buffer. buffer position 0

            int numPages = buffer.getInt(); //gets number of pages
            

            buffer.clear(); //set buffer back to position 0 to read again
            channel.position(4); //set channel to the beginning of page order
            channel.read(buffer); //read in page order
            buffer.flip(); //flip buffer; buffer position is 0

            //get the order of the pages
            List<Integer> pageOrder = new ArrayList<>();
            for (int i = 0; i < numPages; i++){
                pageOrder.add(buffer.getInt());
                //check if adding one more pageid would be greater than the pageSize
                if (buffer.position()+4 > pageSize){
                    //gets difference between how many bytes have been retrieved from
                    //  the buffer and how many bytes are in the buffer
                    int difference = pageSize - buffer.position();
                    //move channel back the size of difference so that
                    //  next thing being read is on a int boundary
                    channel.position(channel.position() - difference);
                    buffer.clear();
                    channel.read(buffer);
                    buffer.flip();
                }
            }
            return pageOrder;
        }
    }*/

    /**
     * Gets a page with the given pageId from the pageBuffer
     * @param tableId the id of table where the page is
     * @param pageId the id of the page to get
     * @param pageOrder the order of the pages in the table with the given table id
     * @return the Page retrieved
     * @throws IOException
     */
    public Page getPage(int tableId, int pageId) throws IOException{
        return (Page)pageBuffer.getPage(tableId, pageId, null, false);
    }

    /**
     * Function for getting BplusNodes from buffer
     */
    public static BplusTreeNode getBplusNode(int tableId, int pageId) throws IOException{
        AttributeSchema primaryKey = null;
        for (AttributeSchema as : catalog.getTableSchemaByNum(tableId).getAttributes()){
            if (as.isPrimaryKey()){
                primaryKey = as;
                break;
            }
        }
        return (BplusTreeNode)pageBuffer.getPage(tableId, pageId, primaryKey, true);
    }

    /**
     * Push BplusNode to buffer
     */
    public static void pushBplusNode(int tableId, int pageId, BplusTreeNode node) throws IOException{
        pageBuffer.pushPage(tableId, pageId, node);
    }

    /**
     * Checks the records of this table to see if any of them contain the given value
     * @param tableId the id of the table to be checking
     * @param tableSchema the schema of the table
     * @param attributeIndex the index of the attribute in the list of attributes for this table that the value belongs to
     * @param value the value to check records for
     * @return true if one of the records contains the value
     * @throws IOException
     */
    public boolean checkForSameValue(int tableId, TableSchema tableSchema, int attributeIndex, String value) throws IOException{
        //List<Integer> pageOrder = getPageOrder(tableId, false);
        List<Integer> pageOrder = Catalog.getTablePageOrder(tableId);
        for (int pageId : pageOrder){
            Page page = getPage(tableId, pageId);
            List<Record> records = page.getRecords();
            for (Record record : records){
                if(record.checkForValue(tableSchema, attributeIndex, value)){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Creates a new table file
     * @param tableId the id of the table
     * @param tablePath the path of where the table file will be located
     * @return fale if error, true otherwise
     * @throws IOException
     */
    //TODO: Combine createNewTableFile and createNewBplusFile together
    public static boolean createNewTableFile(int tableId, String tablePath) throws IOException{
        File tableFile = new File(tablePath);
        if(tableFile.exists()){
            System.err.println("Table"+tableId+" File already exists");
            return false;
        }
        boolean fileCreated = tableFile.createNewFile();
        if (!fileCreated){
            System.err.println("Error with creating Table file");
            return false;
        }

        RandomAccessFile raf = new RandomAccessFile(tableFile, "rw");

        // Guaranteed empty file since we just created it
        // pass in 0 for current number of pages
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(0);
        raf.write(buffer.array());
        raf.close();

        return true;
    }

    /**
     * Creates new BplusTree file
     */
    public static boolean createNewBplusFile(int tableId, String BplusPath) throws IOException{
        File BplusFile = new File(BplusPath);
        if(BplusFile.exists()){
            System.err.println("BplusFile"+tableId+" File already exists");
            return false;
        }
        boolean fileCreated = BplusFile.createNewFile();
        if (!fileCreated){
            System.err.println("Error with creating Table file");
            return false;
        }

        RandomAccessFile raf = new RandomAccessFile(BplusFile, "rw");

        // Guaranteed empty file since we just created it
        // pass in 0 for current number of pages
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(0);
        raf.write(buffer.array());
        raf.close();

        return true;
    }

    /**
     * Deletes the table file at the given path
     * @param tablePath the path where the table file can be found
     * @return true if table was deleted, false otherwise
     * @throws IOException 
     */
    public static boolean deleteTable(int tableId, boolean indexOn) throws IOException{
        String tablePath = dbLocation + "/tables/table" + tableId + ".tbl";
        File fileToDelete = new File(tablePath);
        if(fileToDelete.delete()){
            if(indexOn){
                String treePath = dbLocation + "/indexes/tree" + tableId + ".bpt";
                fileToDelete = new File(treePath);
                if(!fileToDelete.delete()){
                    return false;
                }
                catalog.removeRoot(tableId);
                catalog.saveCatalog();
            }
            return true;
        }
        return false;
    }

    /**
     * Checks if this record needs to be updated based on the where criteria
     * @param record The record to check
     * @param whereClauses the criteria to check the record by
     * @return true if the record needs to be updated
     */
    /*private boolean needsUpdating(Record record, List<String> whereClauses){
        if (whereClauses.isEmpty()){
            return true;
        }
        return false;
    }*/

    /**
     * Updates the values of records that meet the criteria of the where clause
     *  A single value from one of the records attributes is updated
     * @param oldTableId the id of the old table of the records to be updated
     * @param tableSchema the schema for the old table of the records being updated
     * @param attributeIndex the index of a list of the table's attributes that is being updated 
     *                          for each record that needs to be updated
     * @param newValue the value to update the record with
     * @param isUnique if the attribute for the record that is being updated is unique
     * @param whereClause the clause to use to verify if a record needs to be updated
     * @throws IOException
     * @throws Exception
     */
    public void updateRecords(int oldTableId, TableSchema tableSchema, int attributeIndex, String newValue, boolean isUnique, String whereClause) throws IOException, Exception{
        String originalFilePath = dbLocation + "/tables/table" + oldTableId + ".tbl";
        File tableFile = new File(originalFilePath);
        if (!tableFile.exists()){
            System.out.println("table"+oldTableId+" does not exist.");
            return;
        }

        int newTableId = catalog.getLastUsed() + 1;

        String newTablePath = dbLocation + "tables/table" + newTableId + ".tbl";
        boolean tableFileCreated = StorageManager.createNewTableFile(newTableId, newTablePath);
        if (!tableFileCreated){
            System.err.println("Error with creating a new Table File");
            return;
        }


        if(catalog.isIndexOn()){
            String tree_path = dbLocation + "/indexes/tree" + newTableId + ".bpt";
            boolean table_file_created = StorageManager.createNewBplusFile(newTableId, tree_path);
            if (!table_file_created){
                System.err.println("Error with creating a new Table File");
                return;
            }


            AttributeSchema primaryKey = tableSchema.getPrimaryKey();
            AttributeType keyType = primaryKey.getType();

            int keyPointerSize;
            if (keyType == AttributeType.CHAR || keyType == AttributeType.VARCHAR){
                keyPointerSize = primaryKey.getSize()*2 + 8;
            } else{
                keyPointerSize = primaryKey.getSize() + 8;
            }
            int n = (int)Math.floor(((double)pageSize)/keyPointerSize) - 1;

            BplusTreeNode root = new BplusTreeNode(n, 1, newTableId, -1, keyType);
            catalog.setRoot(newTableId, 1);

            //Rewrite the header and push the new page
            //This will make a table's Bplus file always start with one page before inserts begin
            //rewriteTableFileHeader(newTableId, 0, 1, true);
            Catalog.addPageAtIndex(newTableId, 0, 1);
            //List<Integer> pageOrder = getPageOrder(newTableId, true);
            StorageManager.pushBplusNode(newTableId, 1, root);

            Catalog.saveCatalog();
        }

        //List<Integer> oldPageOrder = getPageOrder(oldTableId, false);
        List<Integer> oldPageOrder = Catalog.getTablePageOrder(oldTableId);
        //List<Integer> newPageOrder = new ArrayList<>();
        List<TableSchema> passToWhere = new ArrayList<>();
        passToWhere.add(tableSchema);
        passToWhere.add(tableSchema);

        boolean uniqueRecordChanged = false;

        boolean errorThrown = false;
        String errorMessage = "";

        //For every record in every page, the record is checked to see if it needs
        //  to be updated based on the where clause
        //If a record does, it is updated, and it is inserted into the new table
        //For a unique attribute, if a record is updated, only one can be
        //For all records that will not be changed, they are inserted into the new table normally
        for (int pageId : oldPageOrder){
            Page page = getPage(oldTableId, pageId);
            List<Record> recordsList = page.getRecords();
            for (Record record : recordsList){
                Record newRecord = record;
                //updates record if an error has not been thrown and
                //  if the attribute is unique, the one allowed record has not been changed and
                //Otherwise, just inserts record
                if (!errorThrown && !uniqueRecordChanged){
                    try{
                        //updates record if where clause is empty or the record meets the where clause
                        if (whereClause.equals("") ||
                            DMLFunctions.whereRecord(tableSchema, record, whereClause, passToWhere)){
                            newRecord = record.changeData(newValue, attributeIndex, tableSchema);
                            //if attribute being changed is unique,
                            //  then only one record can be changed
                            if (isUnique){
                                uniqueRecordChanged = true; 
                            }
                        }
                    }catch(Exception e){
                        errorThrown = true;
                        errorMessage = e.getMessage();
                    }
                }

                //if attribute being changed is the primary key, 
                //  then need to insert normally because of possible new order
                //else, can just insert next record at the end
                if (tableSchema.getAttributes().get(attributeIndex).isPrimaryKey()){
                    insertRecord(newRecord, newTableId, false, catalog.isIndexOn());
                } else {
                    insertRecord(newRecord, newTableId, true, catalog.isIndexOn());
                }
            }
        }

        if (uniqueRecordChanged){
            System.out.println("Notice: "+tableSchema.getAttributes().get(attributeIndex).getName()+
                                " is unique. Only one record was updated with value of "+newValue);
        }

        //remove old table file
        deleteTable(oldTableId, catalog.isIndexOn());

        if (errorThrown){
            throw new Exception(errorMessage);
        }
    }
    
    /**
     * Function for inserting using the BplusTree 
     */
    //private List<Integer> insertBplusTree(Record record, int tableId) throws Exception{
    private void insertBplusTree(Record record, int tableId) throws Exception{
        TableSchema tableSchema = catalog.getTableSchemaByNum(tableId);
        Object primaryKeyValue = record.getPrimaryKeyValue(tableSchema);
        AttributeSchema primaryKeyAttributeSchema = null;
        for (AttributeSchema as : catalog.getTableSchemaByNum(tableId).getAttributes()){
            if (as.isPrimaryKey()){
                primaryKeyAttributeSchema = as;
                break;
            }
        }

        //Insert into Bplus tree to get the page and index in the table to insert
        BplusTreeNode root = getBplusNode(tableId, catalog.getRoot(tableId));
        int[] pagePointer = root.addNewValue(primaryKeyValue);
        //treePageOrder = getPageOrder(tableId, true);
        root = getBplusNode(tableId, catalog.getRoot(tableId));
        if (pagePointer == null){
            throw new Exception("Duplicate Value");
        }
        int originalPageID = pagePointer[0];
        int pageIndex = pagePointer[1];

        if (Catalog.getTablePageOrder(tableId) == null){
            Page newPage = new Page(pageSize, 1);
            List<Record> newRecordsList = new ArrayList<>();
            newRecordsList.add(record);
            newPage.setRecords(newRecordsList);
            //List<Integer> newPageOrder = rewriteTableFileHeader(tableId, 0, 1, false);
            Catalog.addPageAtIndex(tableId, 0, newPage.getPageId());
            pageBuffer.pushPage(tableId, 1, newPage);

            return;
        }

        //insert into page at index
        Page page = (Page)pageBuffer.getPage(tableId, originalPageID, primaryKeyAttributeSchema, false);
        page.insertAtIndex(record, pageIndex);

        List<Integer> pageOrder = Catalog.getTablePageOrder(tableId);

        //do splitting of page if necessary
        if(page.pageIsGreaterThanPageSize()){
            Page newPage = new Page(pageSize, Collections.max(pageOrder)+1);
            List<Record> oldPageOrignalList = page.getRecords();
            int numberOfRecords = oldPageOrignalList.size();
            int numberOfRecordsForOldPage = (int)(Math.ceil((double)numberOfRecords)/2.0);
            List<Record> oldPageNewRecords = new ArrayList<>();
            List<Record> newPageRecords = new ArrayList<>();
            for (int i = 0; i < numberOfRecords; i++){
                if (i < numberOfRecordsForOldPage){
                    oldPageNewRecords.add(oldPageOrignalList.get(i));
                } else {
                    newPageRecords.add(oldPageOrignalList.get(i));
                }
            }
            page.setRecords(oldPageNewRecords);
            newPage.setRecords(newPageRecords);

            //update the BplusTree leaf nodes so they point to the new table page and correct indexs
            Object NewRecordsFirstPrimary = newPageRecords.get(0).getPrimaryKeyValue(tableSchema);
            Object NewRecordsLastPrimary = newPageRecords.get(newPageRecords.size() - 1).getPrimaryKeyValue(tableSchema);

            //update the BplusTree leaf nodes
            root.updatePagePointer(NewRecordsFirstPrimary, NewRecordsLastPrimary, newPage.getPageId());

            int pageOrderIndex = pageOrder.indexOf(originalPageID);
            if(pageOrderIndex == -1) {
                // Put it at the end
                pageOrderIndex = pageOrder.size();
            } else {
                pageOrderIndex += 1;
            }

            //List<Integer> newPageOrder = rewriteTableFileHeader(tableId, pageOrderIndex, Collections.max(oldPageOrder)+1, false);
            Catalog.addPageAtIndex(tableId, pageOrderIndex, newPage.getPageId());
            pageBuffer.pushPage(tableId, originalPageID, page);
            pageBuffer.pushPage(tableId, newPage.getPageId(), newPage);
            //return newPageOrder;
        }else{
            pageBuffer.pushPage(tableId, originalPageID, page);
            //return oldPageOrder;
        }
    }

    
    /**
     * Method for inserting a Record into a table
     * @param record the record to insert
     * @param tableId the id of the table to insert record in
     * @param oldPageOrder the page order of the table before record is inserted
     * @param addAtEnd boolean for adding record at the end of file or not;
     *                  False if doing a regular insert call
     *                  True if doing any method that requires copying records from old table to new table (e.g. alter,  etc.)
     *                  Helps to increase execution time
     * @return updated order of page id
     * @throws IOException
     */
    //public List<Integer> insertRecord(Record record, int tableId, List<Integer> oldPageOrder, boolean addAtEnd, boolean indexOn) throws Exception{
    //public List<Integer> insertRecord(Record record, int tableId, boolean addAtEnd, boolean indexOn) throws Exception{
    public void insertRecord(Record record, int tableId, boolean addAtEnd, boolean indexOn) throws Exception{
        //file should always exist and if empty, just has pagesize of 0
        String filePath;
        if (indexOn){
            filePath = dbLocation + "/indexes/tree" + tableId + ".bpt";
        } else {
            filePath = dbLocation + "/tables/table" + tableId + ".tbl";
        }
        //String filePath = dbLocation + "/tables/table" + tableId + ".tbl";
        File tableFile = new File(filePath);
        if (!tableFile.exists()){
            System.out.println("table"+tableId+" does not exist.");
            //return null;
            return;
        }
        //try (RandomAccessFile raf = new RandomAccessFile(filePath, "r"); FileChannel channel = raf.getChannel()) {

            int numPages;
            //List<Integer> pageOrder;
            //List<Integer> treePageOrder = null;
            if (indexOn){
                //treePageOrder = getPageOrder(tableId, true);
                //numPages = treePageOrder.size();
                numPages = Catalog.getTreeNumPages(tableId); //get the number of tree pages
                //pageOrder = null;
            } else {
                //numPages = oldPageOrder.size(); //get the number of pages
                //numPages = Catalog.getTablesNumPages(tableId); //get the number of pages
                List<Integer> pageOrder = Catalog.getTablePageOrder(tableId); //get the number of table pages
                if (pageOrder != null){
                    numPages = pageOrder.size();
                } else{
                    numPages = 0;
                }
                
            }

            /*If the number of pages is 0, a new Page will be created,
             *  the record will be added to the new page, the Table file
             *  header will be rewritten to accomidate the new page,
             *  and the new page will be pushed to the PageBuffer.
            */
            if (numPages == 0){
                Page newPage = new Page(pageSize, 1);
                Catalog.addPageAtIndex(tableId, 0, 1);
                List<Record> newRecordsList = new ArrayList<>();
                newRecordsList.add(record);
                newPage.setRecords(newRecordsList);
                //List<Integer> newPageOrder = rewriteTableFileHeader(tableId, 0, 1, false);
                pageBuffer.pushPage(tableId, newPage.getPageId(), newPage);
                //raf.close();
                return;
            }

            if (indexOn){
                //return insertBplusTree(record, tableId);
                insertBplusTree(record, tableId);
                return;
            }

            int pageOrderIndex = 0; //the current index in the pageOrder array
            boolean wasRecordAdded = false; //used to check if the record was added to a page

            List<Integer> pageOrder = Catalog.getTablePageOrder(tableId);
            //Skips looping through all the pages and goes right to adding to the end
            if (!addAtEnd){
                //Loops through the pageOrder, getting pageIds and seeing if the record can be inserted
                for (int pageId : pageOrder){
                    Page page = (Page)pageBuffer.getPage(tableId, pageId, null, false);
                    wasRecordAdded = page.addRecord(record, catalog.getTableSchemaByNum(tableId));

                    /*
                     * If page was added, checks to see if the current size of the records in
                     *  the page is larger than the max pageSize.
                     * If it is, the Page is split, a new Page is made, each Page
                     *  gets half of the records, the Table file header is rewritten
                     *  to accomadate the new page, and both pages are pushed onto
                     *  the PageBuffer.
                     * Otherwise, the current Page is pushed back onto the page buffer
                     */
                    if (wasRecordAdded){
                        if(page.pageIsGreaterThanPageSize()){
                            Page newPage = new Page(pageSize, Collections.max(pageOrder)+1);
                            List<Record> oldPageOrignalList = page.getRecords();
                            int numberOfRecords = oldPageOrignalList.size();
                            int numberOfRecordsForOldPage = (int)(Math.ceil((double)numberOfRecords)/2.0);
                            List<Record> oldPageNewRecords = new ArrayList<>();
                            List<Record> newPageRecords = new ArrayList<>();
                            for (int i = 0; i < numberOfRecords; i++){
                                if (i < numberOfRecordsForOldPage){
                                    oldPageNewRecords.add(oldPageOrignalList.get(i));
                                } else {
                                    newPageRecords.add(oldPageOrignalList.get(i));
                                }
                            }
                            page.setRecords(oldPageNewRecords);
                            newPage.setRecords(newPageRecords);

                            //List<Integer> newPageOrder = rewriteTableFileHeader(tableId, pageOrderIndex+1, Collections.max(oldPageOrder)+1, false);
                            Catalog.addPageAtIndex(tableId, pageOrderIndex+1, newPage.getPageId()); //add new pageID following the index of the current pageID
                            pageBuffer.pushPage(tableId, pageId, page);
                            pageBuffer.pushPage(tableId, newPage.getPageId(), newPage);

                            //raf.close();
                            return;
                        }
                        pageBuffer.pushPage(tableId, pageId,page);

                        //raf.close();
                        return;
                    }
                    pageBuffer.pushPage(tableId, pageId,page);
                    pageOrderIndex += 1;
                }
            } else {
                pageOrderIndex = pageOrder.size();
            }

            /**
             * Only gets here if the record wasn't inserted into any of the pages
             * The record is then inserted into the last page in the Table,
             *  and the same check to see if the size of the records in the Page
             *  is larger than the max pageSize and if splitting needs to occur
             *  as above
             */
            if (!wasRecordAdded){
                Page page = (Page)pageBuffer.getPage(tableId, pageOrder.get(pageOrder.size()-1), null, false);
                List<Record> oldPageOrignalList = page.getRecords();
                oldPageOrignalList.add(record);
                page.setRecords(oldPageOrignalList);
                if(page.pageIsGreaterThanPageSize()){
                    Page newPage = new Page(pageSize, Collections.max(pageOrder)+1);
                    oldPageOrignalList = page.getRecords();
                    int numberOfRecords = oldPageOrignalList.size();
                    int numberOfRecordsForOldPage = (int)(Math.ceil((double)numberOfRecords)/2.0);
                    List<Record> oldPageNewRecords = new ArrayList<>();
                    List<Record> newPageRecords = new ArrayList<>();
                    for (int i = 0; i < numberOfRecords; i++){
                        if (i < numberOfRecordsForOldPage){
                            oldPageNewRecords.add(oldPageOrignalList.get(i));
                        } else {
                            newPageRecords.add(oldPageOrignalList.get(i));
                        }
                    }
                    page.setRecords(oldPageNewRecords);
                    newPage.setRecords(newPageRecords);

                    //List<Integer> newPageOrder = rewriteTableFileHeader(tableId, pageOrderIndex, Collections.max(oldPageOrder)+1, false);
                    Catalog.addPageAtIndex(tableId, pageOrderIndex, newPage.getPageId()); //add new pageID following the index of the current pageID

                    pageBuffer.pushPage(tableId, page.getPageId(), page);
                    pageBuffer.pushPage(tableId, newPage.getPageId(), newPage);

                    //raf.close();
                    return;
                }
                pageBuffer.pushPage(tableId, page.getPageId(), page);

                //raf.close();
                return;
            }
            //raf.close();
            //return null;
        //}
    }

    /**
     * Removes the attribute with the given name from all records in the
     *  table with the given table id
     * @param attrName the name of the attribute to be removed from all reocrds
     * @param oldTableId the id of the table to manipulate
     * @throws IOException
     */
    public void removeAttribute(String attrName, int oldTableId) throws Exception{
        String originalFilePath = dbLocation + "/tables/table" + oldTableId + ".tbl";
        File tableFile = new File(originalFilePath);
        if (!tableFile.exists()){
            System.out.println("table"+oldTableId+" does not exist.");
            return;
        }

        int newTableId = catalog.getLastUsed() + 1;

        String tablePath = dbLocation + "tables/table" + newTableId + ".tbl";
        boolean tableFileCreated = StorageManager.createNewTableFile(newTableId, tablePath);
        if (!tableFileCreated){
            System.err.println("Error with creating a new Table File");
            return;
        }
        if(catalog.isIndexOn()){
            String tree_path = dbLocation + "/indexes/tree" + newTableId + ".bpt";;
            boolean tree_file_created = StorageManager.createNewBplusFile(newTableId, tree_path);;
            if (!tree_file_created){
                System.err.println("Error with creating a new Tree File");
                return;
            }

            TableSchema schema = catalog.getTableSchemaByNum(oldTableId);
            AttributeSchema primaryKey = schema.getPrimaryKey();
            AttributeType keyType = primaryKey.getType();

            int keyPointerSize;
            if (keyType == AttributeType.CHAR || keyType == AttributeType.VARCHAR){
                keyPointerSize = primaryKey.getSize()*2 + 8;
            } else{
                keyPointerSize = primaryKey.getSize() + 8;
            }
            int n = (int)Math.floor(((double)pageSize)/keyPointerSize) - 1;
            
            BplusTreeNode root = new BplusTreeNode(n, 1, newTableId, -1, keyType);
            //Rewrite the header and push the new page
            //This will make a table's Bplus file always start with one page before inserts begin
            // StorageManager.rewriteTableFileHeader(lastUsedId, 0, 1, true);
            // List<Integer> pageOrder = StorageManager.getPageOrder(lastUsedId, true);
            // StorageManager.pushBplusNode(lastUsedId, 1, root, pageOrder);

            catalog.setRoot(newTableId, 1);
            catalog.saveCatalog();

            //StorageManager.rewriteTableFileHeader(newTableId, 0, 1, true);
            Catalog.addPageAtIndex(newTableId, 0, 1);
            //List<Integer> pageOrder = StorageManager.getPageOrder(newTableId, true);
            StorageManager.pushBplusNode(newTableId, 1, root);
        }

        
        try (RandomAccessFile raf = new RandomAccessFile(originalFilePath, "rw"); FileChannel channel = raf.getChannel()) {

            //List<Integer> pageOrder = getPageOrder(oldTableId, false);
            List<Integer> pageOrder = Catalog.getTablePageOrder(oldTableId);
            //List<Integer> newPageOrder = new ArrayList<>();

            for (int pageId : pageOrder){
                Page oldPage = (Page) pageBuffer.getPage(oldTableId, pageId, null, false);
                List<Record> oldRecords = oldPage.getRecords();
                for (Record record : oldRecords){
                    Record newRecord = record.removeAttribute(attrName, catalog.getTableSchemaByNum(oldTableId));
                    //insertRecord(newRecord, newTableId, pageOrder);
                    insertRecord(newRecord, newTableId, true, catalog.isIndexOn());
                }
            }
        }

        //remove old table file
        deleteTable(oldTableId, catalog.isIndexOn());

    }

    /**
     * Removes all records that satisfy the given where condition in a
     *  table with the given table id
     * @param where_condition condition to check to delete
     * @param oldTableId the id of the table to manipulate
     * @throws IOException
     */
    public void deleteRows(String where_condition, int old_table_id) throws Exception{
        String original_file_path = dbLocation + "/tables/table" + old_table_id + ".tbl";
        File table_file = new File(original_file_path);
        if (!table_file.exists()){
            System.out.println("table"+old_table_id+" does not exist.");
            return;
        }

        int new_table_id = catalog.getLastUsed() + 1;

        String table_path = dbLocation + "tables/table" + new_table_id + ".tbl";
        boolean table_file_created = StorageManager.createNewTableFile(new_table_id, table_path);
        if (!table_file_created){
            System.err.println("Error with creating a new Table File");
            return;
        }
        if(catalog.isIndexOn()){
            String tree_path = dbLocation + "/indexes/tree" + new_table_id + ".bpt";;
            boolean tree_file_created = StorageManager.createNewBplusFile(new_table_id, tree_path);;
            if (!tree_file_created){
                System.err.println("Error with creating a new Tree File");
                return;
            }

            TableSchema schema = catalog.getTableSchemaByNum(old_table_id);
            AttributeSchema primaryKey = schema.getPrimaryKey();
            AttributeType keyType = primaryKey.getType();

            int keyPointerSize;
            if (keyType == AttributeType.CHAR || keyType == AttributeType.VARCHAR){
                keyPointerSize = primaryKey.getSize()*2 + 8;
            } else{
                keyPointerSize = primaryKey.getSize() + 8;
            }
            int n = (int)Math.floor(((double)pageSize)/keyPointerSize) - 1;
            
            BplusTreeNode root = new BplusTreeNode(n, 1, new_table_id, -1, keyType);
            //Rewrite the header and push the new page
            //This will make a table's Bplus file always start with one page before inserts begin
            // StorageManager.rewriteTableFileHeader(lastUsedId, 0, 1, true);
            // List<Integer> pageOrder = StorageManager.getPageOrder(lastUsedId, true);
            // StorageManager.pushBplusNode(lastUsedId, 1, root, pageOrder);

            //TODO:Move saveCatalog to the bottom
            catalog.setRoot(new_table_id, 1);
            catalog.saveCatalog();

            //StorageManager.rewriteTableFileHeader(new_table_id, 0, 1, true);
            Catalog.addPageAtIndex(new_table_id, 0, 1);
            //List<Integer> pageOrder = StorageManager.getPageOrder(new_table_id, true);
            StorageManager.pushBplusNode(new_table_id, 1, root);
        }

        //TODO: Does this cause problems when delete all rows and then adding new value?
        // if no where condition, all are deleted
        if(where_condition.equals("")){
            //remove old table file
            deleteTable(old_table_id, catalog.isIndexOn());
            System.out.println("SUCCESS");
            return;
        }

        boolean error_encountered = false;
        String error_message = "";
        //try (RandomAccessFile raf = new RandomAccessFile(original_file_path, "rw"); FileChannel channel = raf.getChannel()) {

            //List<Integer> page_order = getPageOrder(old_table_id, false);
            List<Integer> page_order = Catalog.getTablePageOrder(old_table_id);
            //List<Integer> new_page_order = new ArrayList<>();
            TableSchema schema = catalog.getTableSchemaByNum(new_table_id);
            List<TableSchema> dud_schemas = new ArrayList<>();
            dud_schemas.add(schema);
            dud_schemas.add(schema);

            for (int page_id : page_order){
                Page old_page = (Page)pageBuffer.getPage(old_table_id, page_id, null, false);
                List<Record> records = old_page.getRecords();
                for (Record record : records){
                    if(error_encountered){
                        insertRecord(record, new_table_id, true, catalog.isIndexOn());
                    }else{
                        try {
                            boolean satisfies_where = DMLFunctions.whereRecord(schema, record, where_condition, dud_schemas);
                            if(!satisfies_where){
                                insertRecord(record, new_table_id, true, catalog.isIndexOn());
                            }
                        } catch (Exception e) {
                            insertRecord(record, new_table_id, true, catalog.isIndexOn());
                            error_message = e.getMessage() + "\nError";
                            error_encountered = true;
                        }
                    }
                }
            }
        //}
        //remove old table file
        deleteTable(old_table_id, catalog.isIndexOn());
        if(error_encountered){
            System.out.println(error_message);
        }else{
            System.out.println("SUCCESS");
        }
    }

    /**
     * Add the given attribute to all records with the value of the given default
     * If default is null, then all records will have the value null for the new attribute
     * Otherwise, all records will have the default value as the value for the new attribute
     * @param newAttribute the new attribute to be added to all records of the given table
     * @param defaultValue the default value for the new attribute
     * @param oldTableId the id of the table whose records are being mutated
     * @throws IOException
     */
    public void addAttribute(AttributeSchema newAttribute, String defaultValue, int oldTableId) throws Exception{
        String originalFilePath = dbLocation + "/tables/table" + oldTableId + ".tbl";
        File tableFile = new File(originalFilePath);
        if (!tableFile.exists()){
            System.out.println("table"+oldTableId+" does not exist.");
            return;
        }

        int newTableId = catalog.getLastUsed() + 1;

        String tablePath = dbLocation + "tables/table" + newTableId + ".tbl";
        boolean tableFileCreated = StorageManager.createNewTableFile(newTableId, tablePath);
        if (!tableFileCreated){
            System.err.println("Error with creating a new Table File");
            return;
        }
        if(catalog.isIndexOn()){
            String tree_path = dbLocation + "/indexes/tree" + newTableId + ".bpt";;
            boolean tree_file_created = StorageManager.createNewBplusFile(newTableId, tree_path);;
            if (!tree_file_created){
                System.err.println("Error with creating a new Tree File");
                return;
            }

            TableSchema schema = catalog.getTableSchemaByNum(oldTableId);
            AttributeSchema primaryKey = schema.getPrimaryKey();
            AttributeType keyType = primaryKey.getType();

            int keyPointerSize;
            if (keyType == AttributeType.CHAR || keyType == AttributeType.VARCHAR){
                keyPointerSize = primaryKey.getSize()*2 + 8;
            } else{
                keyPointerSize = primaryKey.getSize() + 8;
            }
            int n = (int)Math.floor(((double)pageSize)/keyPointerSize) - 1;
            
            BplusTreeNode root = new BplusTreeNode(n, 1, newTableId, -1, keyType);
            //Rewrite the header and push the new page
            //This will make a table's Bplus file always start with one page before inserts begin
            // StorageManager.rewriteTableFileHeader(lastUsedId, 0, 1, true);
            // List<Integer> pageOrder = StorageManager.getPageOrder(lastUsedId, true);
            // StorageManager.pushBplusNode(lastUsedId, 1, root, pageOrder);

            catalog.setRoot(newTableId, 1);
            catalog.saveCatalog();

            //StorageManager.rewriteTableFileHeader(newTableId, 0, 1, true);
            Catalog.addPageAtIndex(newTableId, 0, 1);
            //List<Integer> pageOrder = StorageManager.getPageOrder(newTableId, true);
            StorageManager.pushBplusNode(newTableId, 1, root);
        }

        //try (RandomAccessFile raf = new RandomAccessFile(originalFilePath, "rw"); FileChannel channel = raf.getChannel()) {
            //List<Integer> pageOrder = getPageOrder(oldTableId, false);
            List<Integer> pageOrder = Catalog.getTablePageOrder(oldTableId);
            //List<Integer> newPageOrder = new ArrayList<>();

            for (int pageId : pageOrder){
                Page oldPage = (Page)pageBuffer.getPage(oldTableId, pageId, null, false);
                List<Record> oldRecords = oldPage.getRecords();
                for (Record record : oldRecords){
                    Record newRecord = record.addAttribute(newAttribute, defaultValue, catalog.getTableSchemaByNum(oldTableId));
                    insertRecord(newRecord, newTableId, true, catalog.isIndexOn());
                }
            }
        //}

        //remove old table file
        deleteTable(oldTableId, catalog.isIndexOn());
    }

    /**
     * Method for writing everything in the pageBuffer to file
     */
    public void flushBuffer() throws IOException{
        pageBuffer.flush(0, false);
    }
}
