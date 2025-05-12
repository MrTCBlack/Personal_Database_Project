/**
 * Implementation of the DML Parser of the database handling the insert, select, display_info, and display_schema statements
 * 
 * @author Teagan Harvey tph6529
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class DMLParser {
    
    /**
     * Inserts given values into a given table
     * 
     * @param table_name name of the table
     * @param values 2D arraylist containing values to be inserted
     * @throws IOException 
     */
    public static void insert(Catalog catalog, String table_name, ArrayList<List<String>> records, StorageManager storage_manager) throws IOException{
        TableSchema table_schema = catalog.getTableSchemaByName(table_name);
        // if table doesn't exist, ERROR, else try to insert
        if(table_schema == null){
            System.out.println("No such table " + table_name + "\nERROR");
        }else{
            int table_num = table_schema.getTableNum();
            List<AttributeSchema> attributes = table_schema.getAttributes();

            // grab all page numbers from table
            List<Integer> pageOrder = storage_manager.getPageOrder(table_num, false);

            // loop through user given records
            for(List<String> record : records){
                ArrayList<Object> record_values= new ArrayList<>();
                List<AttributeType> given_attribute_types = new ArrayList<>();
                // loop through each value of a record
                for(String value : record){
                    // save the type of user given value
                    try {
                        record_values.add(Integer.parseInt(value));
                        given_attribute_types.add(AttributeType.INTEGER);
                        continue;
                    } catch (Exception e) {}
                    try {
                        record_values.add(Double.parseDouble(value));
                        given_attribute_types.add(AttributeType.DOUBLE);
                        continue;
                    } catch (Exception e) {}

                    if(value.toLowerCase().equals("true") || value.toLowerCase().equals("false")){
                        record_values.add(Boolean.parseBoolean(value));
                        given_attribute_types.add(AttributeType.BOOLEAN);
                        continue;
                    }else if(value.toLowerCase().equals("null")){
                        given_attribute_types.add(AttributeType.NULL);
                        record_values.add(null);
                    }else if(value.startsWith("\"") && value.endsWith("\"")){
                        given_attribute_types.add(AttributeType.CHAR);
                        record_values.add(value.substring(1, value.length() - 1));
                    }else{
                        print_values(record);
                        System.out.println("Invalid Data type: " + value + "\nERROR");
                        return;
                    }
                }

                // save a list of what the attribute types are supposed to be
                List<AttributeType> schema_attribute_types = new ArrayList<>();
                for(AttributeSchema attribute_schema : attributes){
                    schema_attribute_types.add(attribute_schema.getType());
                }

                // check that there is the correct amount of values
                if(given_attribute_types.size() != attributes.size()){
                    print_values(record);
                    System.out.print("Too many attributes: ");
                    print_expected_error(attributes, given_attribute_types, record);
                    return;
                }
                // loop through each value type given and compare to what it is supposed to be
                for(int i = 0; i < given_attribute_types.size(); i++){
                    // if the types are different, check if value is null and constraint isNotNull
                    AttributeType given_type = given_attribute_types.get(i);
                    AttributeType expected_type = schema_attribute_types.get(i);
                    if(!given_type.equals(expected_type)){
                        if((given_type.equals(AttributeType.NULL) && !attributes.get(i).isNotNull()) || (expected_type.equals(AttributeType.VARCHAR) && given_type.equals(AttributeType.CHAR))){
                            // nothing
                        }else if(given_type.equals(AttributeType.NULL) && attributes.get(i).isNotNull()){
                            print_values(record);
                            System.out.println(attributes.get(i).getName() + " is notNull\nERROR");
                            return;
                        }else{
                            print_values(record);
                            System.out.print("Invalid data type: ");
                            print_expected_error(attributes, given_attribute_types, record);
                            return;
                        }
                    // if the value is the primary key, check that it is not a duplicate
                    }
                    if(attributes.get(i).isPrimaryKey()){
                        if(given_type.equals(AttributeType.NULL)){
                            print_values(record);
                            System.out.println(attributes.get(i).getName() + " is primary key, can't be null\nERROR");
                            return;
                        }
                        String value = "";
                        if(given_type.equals(AttributeType.CHAR)){
                            value = record_values.get(i).toString();
                        }else{
                            value = record.get(i);
                        }
                        if(!catalog.isIndexOn()){
                            Record duplicate = storage_manager.getRecord(table_num, value);
                            if(!(duplicate == null)){
                                ArrayList<Object> converted_values = convert_record_to_values(attributes, duplicate);
                                String message = "Duplicate primary key for ";
                                print_insert_error(message, converted_values, record);
                                return;
                            }
                        }
                    }
                    int expected_length = attributes.get(i).getSize();
                    int given_length = record.get(i).length() - 2;
                    // check that char doesn't go over varchar length
                    if(expected_type.equals(AttributeType.VARCHAR) && given_type.equals(AttributeType.CHAR) && (given_length > expected_length || given_length == 0)){
                        print_values(record);
                        System.out.println("varChar(" + expected_length + ") can accept up to " + expected_length + " chars; " + record.get(i) + " is " + given_length + "\nERROR");
                        return;
                    //check that given char length matches expected char length
                    }else if(expected_type.equals(AttributeType.CHAR) && given_type.equals(AttributeType.CHAR) && given_length != expected_length){
                        print_values(record);
                        System.out.println("char(" + expected_length + ") can only accept " + expected_length + " chars; " + record.get(i) + " is " + given_length + "\nERROR");
                        return;
                    }

                    // check if attribute is unique
                    if(attributes.get(i).isUnique() && !given_type.equals(AttributeType.NULL)){
                        
                        // loop through all pages
                        for(Integer page_num : pageOrder){
                            Page cur_page = storage_manager.getPage(table_num, page_num, pageOrder);
                            List<Record> page_records = cur_page.getRecords();
                            // convert the page's records into printable values
                            for(Record page_record : page_records){
                                ArrayList<Object> converted_values = convert_record_to_values(attributes, page_record);
                                if(record_values.get(i).toString().equals(converted_values.get(i).toString())){
                                    String message = attributes.get(i).getName() + " is unique for ";
                                    print_insert_error(message, converted_values, record);
                                    return;
                                }
                            }
                        }
                    }
                }
                // convert record into byte array
                byte[] data = convert_values_to_record(schema_attribute_types, given_attribute_types, record_values);
                Record inserted_record = new Record(data);
                try {
                    pageOrder = storage_manager.insertRecord(inserted_record, table_num, pageOrder, false, catalog.isIndexOn());
                } catch (Exception e) {
                    System.out.println("Error: " + e.getMessage());
                    //e.printStackTrace();
                    return;
                }
            }
            System.out.println("SUCCESS");
        }
    }

    /**
     * converts a the values of a record into a byte array representation
     * @param attribute_types the attribute types of the record
     * @param record list of values to be converted
     * @return byte[] representation of the record values
     */
    public static byte[] convert_values_to_record(List<AttributeType> expected_attribute_types, List<AttributeType> given_attribute_types, ArrayList<Object> record){
        // get the length of record and its null map
        byte[] null_map = new byte[record.size()];
        int total_space = 0;
        for(int i = 0; i < record.size(); i++){
            AttributeType expected_type = expected_attribute_types.get(i);
            AttributeType given_type = given_attribute_types.get(i);

            // add to total space depending on attribute type
            if(given_type.equals(AttributeType.NULL) || 
                record.get(i).equals("null")){
                null_map[i] = 1;
            }else if(given_type.equals(AttributeType.INTEGER)){
                total_space += 4;
            }else if(given_type.equals(AttributeType.DOUBLE)){
                total_space += 8;
            }else if(given_type.equals(AttributeType.BOOLEAN)){
                total_space += 1;
            }else if(given_type.equals(AttributeType.CHAR) ||
                        given_type.equals(AttributeType.VARCHAR)){
                total_space += String.valueOf(record.get(i)).length() * 2;
                if(expected_type.equals(AttributeType.VARCHAR)){
                    total_space += 4;
                }
            }
        }
        // space for null map
        total_space += record.size();
        
        ByteBuffer buffer = ByteBuffer.allocate(total_space);
        
        // add the null map to the byte array
        for(byte bit : null_map){
            buffer.put(bit);
        }

        // loop through values and insert them into byte array
        for(int i = 0; i < record.size(); i++){
            if (null_map[i] == 0){
                AttributeType expected_type = expected_attribute_types.get(i);
                AttributeType given_type = given_attribute_types.get(i);
                if(given_type.equals(AttributeType.INTEGER)){
                    buffer.putInt((Integer)record.get(i));
                }else if(given_type.equals(AttributeType.DOUBLE)){
                    buffer.putDouble((Double)record.get(i));
                }else if(given_type.equals(AttributeType.BOOLEAN)){
                    String value = String.valueOf(record.get(i));
                    byte[] boolean_bytes = {0, 1};
                    if(value.equals("true")){
                        buffer.put(boolean_bytes[1]);
                    }else{
                        buffer.put(boolean_bytes[0]);
                    }
                }else if(given_type.equals(AttributeType.CHAR) ||
                            given_type.equals(AttributeType.VARCHAR)){
                    if(expected_type.equals(AttributeType.VARCHAR)){
                        buffer.putInt(String.valueOf(record.get(i)).length());
                    }
                    String word = (String)record.get(i);
                    for(char c : word.toCharArray()){
                        buffer.putChar(c);
                    }
                }
            }
        }
        return buffer.array();
    }

    /**
     * Prints out error messages in the form:
     *          <message> expected <atributes> got <atributes>
     *          ERROR
     * @param schema_attribute_types the expected attribute types
     * @param given_attribute_types the user given attribute types
     */
    public static void print_expected_error(List<AttributeSchema> attributes, List<AttributeType> given_attribute_types, List<String> record){
        System.out.print("expected ");
        print_expected_attributes(attributes);
        System.out.print(" got ");
        print_given_attributes(given_attribute_types, record);
        System.out.println(".\nERROR");
    }

    /**
     * Prints out an error message in the form:
     *          row (<given_row_values>): <message> row (<existing_row_values>)
     * @param message the message to print out
     * @param converted_values the existing row values in the database
     * @param record the given row values
     */
    public static void print_insert_error(String message, ArrayList<Object> converted_values, List<String> record){
        print_values(record);
        System.out.print(message);
        boolean flag = true;
        System.out.print("row (");
        for(Object dup_value : converted_values){
            if(flag){
                System.out.print(dup_value.toString());
                flag = false;
            }else{
                System.out.print(" " + dup_value.toString());
            }
        }
        System.out.println(")\nERROR");
    }

    /**
     * Prints out given attributes in the form:
     *          (<attribute 1> <attribute N>)
     * @param given_attribute_types the types to be printed
     * @param record record to be referenced to check char and varchar length
     */
    public static void print_given_attributes(List<AttributeType> given_attribute_types, List<String> record){
        System.out.print("(");
        for(int i = 0; i < record.size(); i++){
            AttributeType type = given_attribute_types.get(i);
            if(i > 0){
                System.out.print(" ");
            }
            System.out.print(type.name().toLowerCase());
            // if char or varchar, print their size
            if(type.equals(AttributeType.VARCHAR) || type.equals(AttributeType.CHAR)){
                System.out.print("(" + (record.get(i).length() - 2) + ")");
            }
        }
        System.out.print(")");
    }

    /**
     * Prints out expected attributes in the form:
     *          (<attribute 1> <attribute N>)
     * @param attributes the attributes to be printed
     */
    public static void print_expected_attributes(List<AttributeSchema> attributes){
        System.out.print("(");
        for(int i = 0; i < attributes.size(); i++){
            AttributeType type = attributes.get(i).getType();
            if(i > 0){
                System.out.print(" ");
            }
            System.out.print(type.name().toLowerCase());
            // if char or varchar, print their size
            if(type.equals(AttributeType.VARCHAR) || type.equals(AttributeType.CHAR)){
                System.out.print("(" + attributes.get(i).getSize() + ")");
            }
        }
        System.out.print(")");
    }

    /**
     * prints the given raw values in the form:
     *          row (<value 1> <value N>): 
     * @param values values to be printed
     */
    public static void print_values(List<String> values){
        boolean flag = true;
        System.out.print("row (");
        for(String value : values){
            if(flag){
                System.out.print(value);
                flag = false;
            }else{
                System.out.print(" " + value);
            }
        }
        System.out.print("): ");
    }

    /**
     * Selects given values from a given table based on where and orderby conditions
     * @param catalog catalog for database
     * @param storage_manager storage manager for database
     * @param select_arguments list of which attributes to select
     * @param from_arguments list of which tables to select from
     * @param where_arguments String of the where conditions to satisfy
     * @param orderby_argument argument to order output by
     * @throws IOException
     */
    public static void select(Catalog catalog, StorageManager storage_manager, String[] select_arguments, String[] from_arguments, String where_arguments, String orderby_argument) throws Exception{
        List<TableSchema> to_delete_schemas = new ArrayList<>();
        List<TableSchema> from_table_schemas = new ArrayList<>();
        for(String table_name : from_arguments){
            TableSchema table_schema = catalog.getTableSchemaByName(table_name);
            // if table doesn't exist, ERROR, else add table
            if(table_schema == null){
                System.out.println("No such table " + table_name + "\nERROR");
                return;
            }else{
                from_table_schemas.add(table_schema);
            }
        }

        // perform "from" function to combine given tables
        TableSchema combined_schema = DMLFunctions.select_from(catalog, storage_manager, from_table_schemas);
        to_delete_schemas.add(combined_schema);
        
        try {
            // where function
            TableSchema whereSchema = null;
            if(where_arguments.equals("")){
                whereSchema = combined_schema;
            }else{
                whereSchema = DMLFunctions.where(combined_schema, where_arguments, catalog, storage_manager, from_table_schemas);
                to_delete_schemas.add(whereSchema);
            }

            // orderby function
            TableSchema orderbySchema = null;
            if(orderby_argument.equals("")){
                orderbySchema = whereSchema;
            }else{
                orderbySchema = DMLFunctions.select_orderby(catalog, storage_manager, whereSchema, orderby_argument);
                to_delete_schemas.add(orderbySchema);
            }
            // select function
            TableSchema selectSchema = DMLFunctions.select_select(catalog, storage_manager, orderbySchema, select_arguments);
            to_delete_schemas.add(selectSchema);

            // print final table
            print_table(selectSchema, storage_manager);
            System.out.println("\nSUCCESS");

        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("ERROR");
        } finally {
             // remove all temp tables
            for(TableSchema cur_schema : to_delete_schemas){
                //System.out.println("Removing table: " + cur_schema.getTableName()); // TESTING FOR FROM
                catalog.removeTableByName(cur_schema.getTableName());
            }
            catalog.saveCatalog();
        }
    }

    /**
     * Prints out a table in the form:
     *      -------------------------------
     *      |  attribute1  |  attributeN  |
     *      -------------------------------
     *      |  value1      |  value1      |
     *      |  valueN      |  valueN      |
     * @param table_schema the schema of the table to print
     * @param storage_manager the storage manager to use
     */
    public static void print_table(TableSchema table_schema, StorageManager storage_manager) throws IOException{
        int table_num = table_schema.getTableNum();
            List<AttributeSchema> attributes = table_schema.getAttributes();

            // calculate column widths and print header
            List<Integer> column_width = calculate_column_width(attributes);
            print_header(attributes, column_width);
    
            // grab all page numbers from table
            List<Integer> page_nums = storage_manager.getPageOrder(table_num, false);
            // loop through all pages
            for(Integer page_num : page_nums){
                Page cur_page = storage_manager.getPage(table_num, page_num, page_nums);
                List<Record> page_records = cur_page.getRecords();
                // convert the page's records into printable values
                for(Record record : page_records){
                    ArrayList<Object> converted_values = convert_record_to_values(attributes, record);
                    // print the row
                    print_row(converted_values, column_width);
                }
            }
    }

    /**
     * calculates the width needed for each column of a table
     * @param attributes the attribute schemas of the table
     * @return a list of integers corresponding to the widths needed for each column
     */
    public static List<Integer> calculate_column_width(List<AttributeSchema> attributes){
        int attribute_count = attributes.size();
        List<Integer> column_width = new ArrayList<>();

        // loop through each attribute and either make length of name or default attribute type length
        // if the length of name is too small
        for(int i = 0; i < attribute_count; i++){
            AttributeSchema cur_attribute = attributes.get(i);
            int cur_length = cur_attribute.getName().length();
            AttributeType cur_type = cur_attribute.getType();
            if(cur_type.equals(AttributeType.INTEGER)){
                if(cur_length < 5){
                    column_width.add(5);
                }else{
                    column_width.add(cur_length);
                }
            }else if(cur_type.equals(AttributeType.DOUBLE)){
                if(cur_length < 6){
                    column_width.add(7);
                }else{
                    column_width.add(cur_length);
                }
            }else if(cur_type.equals(AttributeType.BOOLEAN)){
                if(cur_length < 5){
                    column_width.add(5);
                }else{
                    column_width.add(cur_length);
                }
            }else if(cur_type.equals(AttributeType.CHAR) || cur_type.equals(AttributeType.VARCHAR)){
                int char_size = cur_attribute.getSize();
                if(cur_length < char_size){
                    column_width.add(char_size);
                }else{
                    column_width.add(cur_length);
                }
            }
        }
        return column_width;
    }

    /**
     * prints the header of a table in the form:
     *      -------------------------------
     *      |  attribute1  |  attributeN  |
     *      -------------------------------
     * @param attributes the attribute schemas of the table
     * @param column_width list of the column width for each attribute
     */
    public static void print_header(List<AttributeSchema> attributes, List<Integer> column_width){
        int attribute_count = attributes.size();
        // print the seperator
        print_seperator(column_width);

        // print the header of the table
        System.out.println();
        for(int i = 0; i < attribute_count; i++){
            String attribute_name = attributes.get(i).getName();
            System.out.printf("| %-" + column_width.get(i) + "s ", attribute_name);
        }
        System.out.print("|");

        // print the seperator
        print_seperator(column_width);
        System.out.println();
    }

    /**
     * prints the seperator portion of a table in the form:
     *          ---------------------
     * @param column_width widths of each column of the table
     */
    public static void print_seperator(List<Integer> column_width){
        // print the seperator
        System.out.println("");
        for (int width : column_width) {
            for (int i = 0; i < (width + 4); i++) {
                System.out.print("-");
            }
        }
    }

    /**
     * prints a row of a table in the form:
     *      |  value1      |  valueN      |
     * @param values the values of the row to print
     * @param column_width widths of each column of the table
     */
    public static void print_row(ArrayList<Object> values, List<Integer> column_width){
        // print the data of the table
        int attribute_count = values.size();
        for(int i = 0; i < attribute_count; i++){
            System.out.printf("| %-" + column_width.get(i) + "s ", values.get(i));
        }
        System.out.println("|");
    }

    /**
     * converts a record into an arraylist of values converted into their respective types
     * @param attributes List of the attribute schemas
     * @param record the record to be converted
     * @return a list of the converted values
     */
    public static ArrayList<Object> convert_record_to_values(List<AttributeSchema> attributes, Record record){
        ByteBuffer buffer = ByteBuffer.allocate(record.computeSize());
        record.writeToBuffer(buffer);

        ArrayList<Object> converted_values = new ArrayList<Object>();
        int num_attributes = attributes.size();
        byte[] null_map = new byte[num_attributes];

        // move past size of record
        buffer.position(4);
        
        // get the null map for record
        for(int i = 0; i < num_attributes; i++){
            null_map[i] = buffer.get();
        }
        // loop through record and convert into their respective types
        for(int i = 0; i < num_attributes; i++){
            // if attribute empty, mark as null
            if(null_map[i] == 1){
                converted_values.add("null");
                continue;
            }
            AttributeType type = attributes.get(i).getType();
            String word = "";
            int word_length = 0;
            switch (type) {
                case INTEGER:
                    converted_values.add(buffer.getInt());
                    break;
                case DOUBLE:
                    converted_values.add(buffer.getDouble());
                    break;
                // 0 = false; 1 = true
                case BOOLEAN:
                    byte bool = buffer.get();
                    if(bool == 0){
                        converted_values.add("false");
                        //converted_values.add(false);
                    }else{
                        converted_values.add("true");
                        //converted_values.add(true);
                    }
                    break;
                // get word length from attribute schema
                case CHAR:
                    word_length = attributes.get(i).getSize();
                    for(int j = 0; j < word_length; j++){
                        word += buffer.getChar();
                    }
                    converted_values.add(word);
                    break;
                // get word length stored in record
                case VARCHAR:
                    word_length = buffer.getInt();
                    for(int j = 0; j < word_length; j++){
                        word += buffer.getChar();
                    }
                    converted_values.add(word);
                    break;
                // should never get here
                default:
                    System.out.println("Invalid attribute type");
                    break;
            }
        }
        return converted_values;
    }

     /**
      * display info of a given table
      * @param catalog catalog of the database
      * @param table_name name of table to display
      * @param storage_manager storage manager of the database
      * @throws IOException
      */
    public static void display_info(Catalog catalog, String table_name, StorageManager storage_manager) throws IOException{

        TableSchema table_schema = catalog.getTableSchemaByName(table_name);

        // if table doesn't exist, ERROR, else display table schema
        if(table_schema == null){
            System.out.println("No such table " + table_name + "\nERROR");
        }else{
                print_table_schema(table_schema, storage_manager);
                System.out.println("SUCCESS");
        }
    }

    /**
     * prints out a table in the form:
     *      Table name: <name>
     *      Table schema:
     *          <attribute 1>
     *          <attribute N>
     *      Pages: <Page Count>
     *      Records: <Records Count>
     * @param table_schema table schema to be printed
     * @param storage_manager storage manager of the database
     * @throws IOException
     */
     public static void print_table_schema(TableSchema table_schema, StorageManager storage_manager) throws IOException{
        System.out.println("Table name: " + table_schema.getTableName());
        System.out.println("Table schema: ");

        // loop through attributes and print attribute schema
        List<AttributeSchema> attributes = table_schema.getAttributes();
        for(AttributeSchema attribute : attributes){
            AttributeType type = attribute.getType();
            System.out.print("\t" + attribute.getName() + ":" + type.name().toLowerCase());
            if(type.equals(AttributeType.VARCHAR) || type.equals(AttributeType.CHAR)){
                System.out.print("(" + attribute.getSize() + ")");
            }
            if(attribute.isPrimaryKey()){
                System.out.print(" primarykey");
            }else{
                if(attribute.isNotNull()){
                        System.out.print(" notNull");
                }
                if(attribute.isUnique()){
                    System.out.print(" unique");
                }
            }
            System.out.println();
        }
        int table_num = table_schema.getTableNum();
        List<Integer> page_nums = storage_manager.getPageOrder(table_num, false);
        int num_pages = page_nums.size();
        int num_records = 0;
        for(int page_num : page_nums){
            Page cur_page = storage_manager.getPage(table_num, page_num, page_nums);
            num_records += cur_page.getNumRecords();
        }
         
        System.out.println("Pages: " + num_pages);
        System.out.println("Records: " + num_records);
    }

    /**
     * displays the schema of the database in the form:
     *          DB location: <DB_location>
     *          Page Size: <page_size>
     *          Buffer Size: <buffer_size>
     * 
     *          Tables:
     * 
     *          <Table 1>
     *          <Table N>
     * @param catalog catalog of the database
     * @param db_loc location of the database
     * @param bufferSize buffersize
     * @param storage_manager storage manager of the database
     * @throws IOException
     */
    public static void display_schema(Catalog catalog, String db_loc, int bufferSize, StorageManager storage_manager) throws IOException{
        System.out.println("DB location: " + db_loc);
        System.out.println("Page Size: " + catalog.getPageSize());
        System.out.println("Buffer Size: " + bufferSize);
        System.out.println("Indexing On: " + catalog.isIndexOn() + "\n");

        Map<String, TableSchema> tableSchemas = catalog.getTableSchemaNameMap();
        
        // if there are no tables, ERROR, else, display all tables
        if(tableSchemas.isEmpty()){
            System.out.println("No tables to display\nSUCCESS");
        }else{
            System.out.println("Tables:");
            for(String key : tableSchemas.keySet()){
                System.out.println();
                print_table_schema(tableSchemas.get(key), storage_manager);
            }
            System.out.println("SUCCESS");
        }
    }


    /**
     * deletes rows from a given table depending on given conditions
     * @param catalog the catalog of the database
     * @param table_name name of the table to delete from
     * @param where_arguments conditions to base deletion on
     * @param storage_manager storage manager of the database
     * @throws IOException 
     */
    public static void delete(Catalog catalog, String table_name, String where_arguments, StorageManager storage_manager) throws Exception{
        TableSchema table_schema = catalog.getTableSchemaByName(table_name);

        // if table doesn't exist, ERROR, else delete from table schema
        if(table_schema == null){
            System.out.println("No such table " + table_name + "\nERROR");
        }else{
            // delete all records that satisfy where conditions
            DMLFunctions.delete(catalog, storage_manager, table_name, where_arguments);
        }
    }
}
