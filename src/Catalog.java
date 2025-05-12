import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Manages the database catalog, which keeps track of all table schemas.
 * The catalog is stored in a binary file and contains information about table structures.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 * @Contributor Tyler Black, tcb8683
 */
public class Catalog {
    private static String catalogFile; // Path to the catalog file
    private static int pageSize; // Size of database pages
    private static Map<Integer, TableSchema> tableSchemasByNum; // Stores table schemas by table number
    private static Map<String, TableSchema> tableSchemasByName; // Stores table schemas by name
    private static Map<Integer, Integer> treeNodes; //stores root node pointer for each table
    private int lastUsedId;
    private static boolean indexOn;

    /**
     * construtor for the catalog
     * @param catalogFile file path of catalog
     * @param pageSize pagesize of database
     */
    public Catalog(String catalogFile, int pageSize, boolean indexOn) {
        this.catalogFile = catalogFile;
        this.pageSize = pageSize;
        this.tableSchemasByNum = new HashMap<>();
        this.tableSchemasByName = new HashMap<>();
        this.treeNodes = new HashMap<>();
        this.indexOn = indexOn;
    }

    /**
     * Loads the catalog from a binary file, retrieving existing table schemas.
     * If the file doesn't exist, a new catalog is created.
     */
    public void loadCatalog() throws IOException {
        File file = new File(catalogFile);
        if (!file.exists()) {
            StorageManager.createNewCatalogFile(file, pageSize, indexOn);
            setLastUsed(0);
            return;
        }

        //Calls StorageManagers loadCatalogFromFile to
        //  load in the table schemas and get the original pageSize
        this.pageSize = StorageManager.loadCatalogFromFile(tableSchemasByNum, tableSchemasByName, treeNodes, file);
        if(tableSchemasByNum.size() > 0) {
            setLastUsed(Collections.max(tableSchemasByNum.keySet()));
        } else {
            setLastUsed(0);
        }
    }

    /**
     * Saves the catalog information to a binary file, storing table schemas.
     */
    public static void saveCatalog() throws IOException {
        StorageManager.saveCatalog(catalogFile, pageSize, tableSchemasByNum, tableSchemasByName, treeNodes, indexOn);
    }

    /**
     * Adds a table schema to the catalog.
     */
    public void addTable(TableSchema schema, boolean givenIndexOn) throws IOException {
        tableSchemasByNum.put(lastUsedId+1, schema);
        lastUsedId += 1;
        tableSchemasByName.put(schema.getTableName(), schema);

         BplusTreeNode root = null;
        if(givenIndexOn){
            // 4.5 calculate n for the BplusTreeNode
            AttributeSchema primaryKey = schema.getPrimaryKey();
            AttributeType keyType = primaryKey.getType();

            int keyPointerSize;
            if (keyType == AttributeType.CHAR || keyType == AttributeType.VARCHAR){
                keyPointerSize = primaryKey.getSize()*2 + 8;
            } else{
                keyPointerSize = primaryKey.getSize() + 8;
            }
            int n = (int)Math.floor(((double)pageSize)/keyPointerSize) - 1;
            
            root = new BplusTreeNode(n, 1, lastUsedId, -1, keyType);
            treeNodes.put(lastUsedId, 1);
        }

        // This will automatically update the catalog as soon as a new table schema is added
        saveCatalog();

        String DBPath = catalogFile.substring(0, catalogFile.length() - 7);

        String tablePath = DBPath + "tables/table" + lastUsedId + ".tbl";
        boolean tableFileCreated = StorageManager.createNewTableFile(lastUsedId, tablePath);
        if (!tableFileCreated){
            System.err.println("Error with creating a new Table File");
        }

        if(givenIndexOn){
            String treePath = DBPath + "/indexes/tree" + lastUsedId + ".bpt";
            boolean treeFileCreated = StorageManager.createNewBplusFile(lastUsedId, treePath);
            if (!treeFileCreated){
                System.err.println("Error with creating a new Tree File");
            }
            //Rewrite the header and push the new page
            //This will make a table's Bplus file always start with one page before inserts begin
            StorageManager.rewriteTableFileHeader(lastUsedId, 0, 1, true);
            List<Integer> pageOrder = StorageManager.getPageOrder(lastUsedId, true);
            StorageManager.pushBplusNode(lastUsedId, 1, root, pageOrder);
        }
    }

    /**
     * Retrieves the schema of a table by the tableID.
     */
    public TableSchema getTableSchemaByNum(int tableID) {
        return tableSchemasByNum.get(tableID);
    }

    /**
     * Retrieves the schema of a table by the tableID.
     */
    public TableSchema getTableSchemaByName(String name) {
        return tableSchemasByName.get(name);
    }
    public Map<String, TableSchema> getTableSchemaNameMap(){
        return tableSchemasByName;
    }
    
    /**
     * Retrieves the number of the tables currently in the database
     * @return 
     */
    public int getNumTables() {
        return tableSchemasByNum.size();
    }

    /**
     * Retrieves the page size of the database
     * @return page size of the database as an int
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Updates the last used table id with the max table id used in the database
     */
    public void setLastUsed(int maxTableId) {
        this.lastUsedId = maxTableId;
    }

    /**
     * Returns the last used id value for tables
     */
    public int getLastUsed() {
        return this.lastUsedId;
    }

    /**
     * Creates a new version of the old TableSchema, with the
     *  only difference is that the new version doesn't have the
     *  attribute with the name given
     * @param columnName the name of the attribute to remove
     * @param tableName the name of the table that is being altered
     * @return the new version of the table schema
     */
    public TableSchema removeColumn(String columnName, String tableName){
        TableSchema oldTableSchema = getTableSchemaByName(tableName);
        TableSchema newTableSchema = new TableSchema(tableName, getLastUsed()+1);

        for (AttributeSchema attributeSchema : oldTableSchema.getAttributes()){
            if (!attributeSchema.getName().equals(columnName)){
                newTableSchema.addAttribute(attributeSchema);
            }
        }

        tableSchemasByNum.put(getLastUsed() + 1, newTableSchema);
        return newTableSchema;
    }

    /**
     * Creates a new version of the old TableSchema
     * @param tableName the name of the table that is being altered
     * @return the new version of the table schema
     */
    public TableSchema duplicateSchema(String tableName){
        TableSchema oldTableSchema = getTableSchemaByName(tableName);
        TableSchema newTableSchema = new TableSchema(tableName, getLastUsed()+1);

        for (AttributeSchema attributeSchema : oldTableSchema.getAttributes()){
                newTableSchema.addAttribute(attributeSchema);
        }

        tableSchemasByNum.put(getLastUsed() + 1, newTableSchema);
        return newTableSchema;
    }

    /**
     * Creates a new version of the old TableSchema, with the
     *  only difference being that the new version has the added given attribute
     * @param newAttr the attribute to add
     * @param tableName the name of the table that is being altered
     * @return the new version of the table schema
     */
    public TableSchema addColumn(AttributeSchema newAttr, String tableName){
        TableSchema oldTableSchema = getTableSchemaByName(tableName);
        TableSchema newTableSchema = new TableSchema(tableName, getLastUsed()+1);

        for (AttributeSchema attributeSchema : oldTableSchema.getAttributes()){
            newTableSchema.addAttribute(attributeSchema);
        }
        newTableSchema.addAttribute(newAttr);

        tableSchemasByNum.put(getLastUsed() + 1, newTableSchema);
        return newTableSchema;
    }

    /**
     * updates the catlog by replacing an old tableSchema
     * @param schema the schema to be replaced with
     */
    public void updateTableSchema(TableSchema oldSchema, TableSchema newSchema) {
        // Remove the old schema
        tableSchemasByName.remove(oldSchema.getTableName());
        tableSchemasByNum.remove(oldSchema.getTableNum());
        // Replace with the new schema
        tableSchemasByName.put(newSchema.getTableName(), newSchema);
        int newTableId = getLastUsed() + 1;
        setLastUsed(newTableId);
    }

    /**
     * removes a table with the given name
     * @param tableName the name of the table to be removed
     * @throws IOException 
     */
    public void removeTableByName(String tableName) throws IOException {
        // Grab the table schema
        TableSchema removedTable = tableSchemasByName.get(tableName);
        int tableId = removedTable.getTableNum();

        String tablePath = catalogFile.substring(0, catalogFile.length() - 7);

        tablePath = tablePath + "tables/table" + tableId + ".tbl";

        boolean tableFileDeleted;
        if(tableName.contains("temp")){
            tableFileDeleted = StorageManager.deleteTable(tableId, false);
        }else{
            tableFileDeleted = StorageManager.deleteTable(tableId, indexOn);
        }
        // call storage manager to delete it from memory;
        if (!tableFileDeleted){
            System.err.println("Error with deleting Table File");
            return;
        }

        // Remove it from the maps
        tableSchemasByName.remove(tableName);
        tableSchemasByNum.remove(removedTable.getTableNum());
    }

    /**
     * returns the root node of the given tableID
     * 
     * @param treeID the id of the tree
     * @return the root node of the tree
     */
    public Integer getRoot(int treeID){
        return treeNodes.get(treeID);
    }

    /**
     * add a tree id, root pointer pair and save catalog
     */
    public static void setRoot(int treeID, int pointer) throws IOException{
        treeNodes.put(treeID, pointer);
        saveCatalog();
    }

    /**
     * returns whether indexing is on for the database
     * 
     * @return wether indexing is on
     */
    public static boolean isIndexOn(){
        return indexOn;
    }

    /**
     * sets indexing to be on or off
     */
    public  static void setIndex(boolean isOn){
        indexOn = isOn;
    }

    /**
     * remove a tree id, root pointer pair
     */
    public void removeRoot(int treeId){
        treeNodes.remove(treeId);
    }
}
