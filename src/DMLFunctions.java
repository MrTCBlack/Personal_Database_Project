/**
 * Implementation of the DML Functions of the database handling the select (select, from, where, orderby), delete, and update statements
 * 
 * @author Teagan Harvey tph6529
 * @author Brayden Mossey bjm9599
 */

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DMLFunctions {

    private static class WhereNode{

        String val;
        WhereNode left, right;

        WhereNode(String val){

            this.val = val;

        }

        WhereNode(String val, WhereNode left, WhereNode right){

            this.val = val;
            this.left = left;
            this.right = right;

        }

    }

    private static final Pattern TOKEN_PATTERN = Pattern.compile(

        "(\\b\\w+\\.\\w+\\b)|" +  // Matches "table.column"
        "(\\b\\w+\\b)|" +          // Matches words (identifiers, keywords)
        "([<>!=]=|[<>]|=)|" +        // Matches relational operators
        "([(),;*])"                // Matches punctuation & symbols

    );

    /**
     * performs the "from" function of a select statement. combines given tables and returns new table schema
     * @param catalog catalog of database to use
     * @param storage_manager storage manager of database to use
     * @param table_schemas the table schema to combine
     * @return new table schema of the combined tables
     * @throws IOException
     */
    public static TableSchema select_from(Catalog catalog, StorageManager storage_manager, List<TableSchema> table_schemas) throws Exception{
        // if selecting from only one table, select from single function
        if(table_schemas.size() == 1){
            return select_from_single(catalog, storage_manager, table_schemas.get(0));
        }
        
        // combine the first two tables
        TableSchema combined_schema = combine_tables(catalog, storage_manager, table_schemas.get(0), table_schemas.get(1), true);
        List<TableSchema> to_delete = new ArrayList<>();
        to_delete.add(combined_schema);
        // loop through all other tables and combine them
        for(int i = 2; i < table_schemas.size(); i++){
            combined_schema = combine_tables(catalog, storage_manager, combined_schema, table_schemas.get(i), false);
            to_delete.add(combined_schema);
        }

        // delete any temporary tables
        for(int j = 0; j < to_delete.size() - 1; j++){
            String table_name = to_delete.get(j).getTableName();
            catalog.removeTableByName(table_name);
        }
        catalog.saveCatalog();
        return combined_schema;
    }

    /**
     * performs the "from" function of a select statement on a single table
     * @param catalog catalog of database to use
     * @param storage_manager storage manager of database to use
     * @param table the table schema to use
     * @return new table schema of the single table
     * @throws IOException
     */
    public static TableSchema select_from_single(Catalog catalog, StorageManager storage_manager, TableSchema table) throws Exception{
        // create temporary table
        TableSchema new_table = new TableSchema("from_temp" + catalog.getLastUsed() + 1, catalog.getLastUsed() + 1);
        // go through all attributes, renaming with prefix "{tableName}." before each attribute name and add to new table schema
        for(AttributeSchema attr : table.getAttributes()){
            String new_attr_name = table.getTableName() + "." + attr.getName();
            AttributeSchema cur_attr = new AttributeSchema(new_attr_name, attr.getType(), attr.getSize(), false, false, false);
            new_table.addAttribute(cur_attr);
        }

        catalog.addTable(new_table, false);
        //List<Integer> page_nums = storage_manager.getPageOrder(table.getTableNum(), false);
        List<Integer> page_nums = Catalog.getTablePageOrder(table.getTableNum());
        // loop through all pages
        for(Integer page_num : page_nums){
            Page cur_page = storage_manager.getPage(table.getTableNum(), page_num);
            List<Record> page_records = cur_page.getRecords();
            // copy over all records from previous table
            for(Record record : page_records){
                //List<Integer> new_table_page_nums = storage_manager.getPageOrder(new_table.getTableNum(), false);
                storage_manager.insertRecord(record, new_table.getTableNum(), true, false);
            }
        }
        return new_table;
    }

    /**
     * combines two given tables into one table and returns the new table schema
     * @param catalog catalog of the database to use
     * @param storage_manager storage manager of the database to use
     * @param table1 the first table to be combined
     * @param table2 the second table to be combined
     * @param first_time boolean flag if the first table attribute schemas need to be renamed or not
     * @return new table schema of the combined tables
     * @throws IOException
     */
    public static TableSchema combine_tables(Catalog catalog, StorageManager storage_manager, TableSchema table1, TableSchema table2, boolean first_time) throws Exception{
        // create temporary table
        int combined_table_num = catalog.getLastUsed() + 1;
        TableSchema combined_table = new TableSchema("from_temp" + combined_table_num, combined_table_num);
        String table1_name = table1.getTableName();
        String table2_name = table2.getTableName();

        // loop through table1's attributes, renaming with prefix "{tableName}." before each attribute name (only done if first time flag is true)
        // add each attribute to new table schema
        for(AttributeSchema table1_attr : table1.getAttributes()){
            String new_attr_name = "";
            if(first_time){
                new_attr_name = table1_name + "." + table1_attr.getName();
            }else{
                new_attr_name = table1_attr.getName();
            }
            AttributeSchema cur_attr = new AttributeSchema(new_attr_name, table1_attr.getType(), table1_attr.getSize(), false, false, false);
            combined_table.addAttribute(cur_attr);
        }
        // go through table2's attributes, renaming with prefix "{tableName}." before each attribute name and add to new table schema
        for(AttributeSchema table2_attr : table2.getAttributes()){
            String new_attr_name = table2_name + "." + table2_attr.getName();
            AttributeSchema cur_attr = new AttributeSchema(new_attr_name, table2_attr.getType(), table2_attr.getSize(), false, false, false);
            combined_table.addAttribute(cur_attr);
        }

        catalog.addTable(combined_table, false);
        
        int table1_num = table1.getTableNum();
        //List<Integer> table1_page_nums = storage_manager.getPageOrder(table1.getTableNum(), false);
        List<Integer> table1_page_nums = Catalog.getTablePageOrder(table1_num);
        //int table1_num = table1.getTableNum();
        int table2_num = table2.getTableNum();
        //List<Integer> table2_page_nums = storage_manager.getPageOrder(table2.getTableNum(), false);
        List<Integer> table2_page_nums = Catalog.getTablePageOrder(table2_num);
        //int table2_num = table2.getTableNum();

        // perform a block nested-loop join on the two tables
        for(int table1_page_num : table1_page_nums){
            Page table1_cur_page = storage_manager.getPage(table1_num, table1_page_num);
            List<Record> table1_page_records = table1_cur_page.getRecords();
            for(int table2_page_num : table2_page_nums){
                Page table2_cur_page = storage_manager.getPage(table2_num, table2_page_num);
                List<Record> table2_page_records = table2_cur_page.getRecords();
                for(Record table1_record : table1_page_records){
                    for(Record table2_record : table2_page_records){

                        // get raw data from both table's records
                        byte[] table1_data = table1_record.getData();
                        byte[] table2_data = table2_record.getData();

                        byte[] combined_data = new byte[table1_data.length + table2_data.length];

                        // get sizes for both records null bit maps
                        int table1_map_length = table1.getAttributes().size();
                        int table2_map_length = table2.getAttributes().size();
                        int total_map_length = table1_map_length + table2_map_length;

                        // move null bit maps from both records to start of new record
                        System.arraycopy(table1_data, 0, combined_data, 0, table1_map_length);
                        System.arraycopy(table2_data, 0, combined_data, table1_map_length, table2_map_length);
                        // move values from both records into new record
                        System.arraycopy(table1_data, table1_map_length, combined_data, total_map_length, table1_data.length - table1_map_length);
                        System.arraycopy(table2_data, table2_map_length, combined_data, total_map_length + (table1_data.length - table1_map_length), table2_data.length - table2_map_length);

                        // insert combined record into combined table
                        Record combined_record = new Record(combined_data);
                        //List<Integer> combined_table_page_nums = storage_manager.getPageOrder(combined_table_num, false);
                        storage_manager.insertRecord(combined_record, combined_table_num, true, false);
                    }
                }
            }
        }
        return combined_table;
    }

    /**
     * where helper function for tokenizing input given after "WHERE"
     * @param input string of input given after "WHERE"
     * @return list of tokens to create parse tree from
     */
    public static List<String> tokenizeWhere(String input){

        List<String> tokens = new ArrayList<>();
        Matcher matcher = TOKEN_PATTERN.matcher(input);

        while(matcher.find()){

            String token = matcher.group();
            tokens.add(token);

        }

        return tokens;

    }

    /**
     * parses a where statement into a tree
     * @param tokens list of tokens to parse
     * @return the root note of the tree
     */
    public static WhereNode parseWhere(List<String> tokens){

        Deque<WhereNode> operands = new ArrayDeque<>();
        Deque<String> operators = new ArrayDeque<>();

        for(int i = 0; i < tokens.size(); i++){

            String token = tokens.get(i);

            if(isOperator(token)){

                while (!operators.isEmpty() && wherePrecedence(operators.peek()) >= wherePrecedence(token)){

                    processOperator(operators.pop(), operands);

                }

                operators.push(token);

            }else if(token.equals("(")){

                operators.push(token);

            }else if(token.equals(")")){

                while(!operators.isEmpty() && !operators.peek().equals("(")){

                    processOperator(operators.pop(), operands);

                }

                operators.pop(); 

            }else{

                operands.push(new WhereNode(token));

            }
        }

        while(!operators.isEmpty()){

            processOperator(operators.pop(), operands);

        }

        return operands.pop();

    }

    /**
     * helper function for where; checks if a token is an operator
     * @param token token to check
     * @return if it is an operator or not
     */
    private static boolean isOperator(String token){

        return Arrays.asList("=", "!=", ">", "<", ">=", "<=", "and", "or").contains(token);

    }

    /**
     * helper function for where; checks the precedence of given operator
     * @param operator operator to check
     * @return the precedence of the given operator (0, 1 or 2)
     */
    private static int wherePrecedence(String operator){

        return switch (operator){

            case "and" -> 1;
            case "or" -> 0;
            default -> 2; 

        };

    }

    /**
     * helper function for where; processes next operator from tree
     * @param operator operator to process
     * @param operands operands to perform operator on
     */
    private static void processOperator(String operator, Deque<WhereNode> operands){

        WhereNode right = operands.pop();
        WhereNode left = operands.pop();
        operands.push(new WhereNode(operator, left, right));

    }

    /**
     * checks if a record satisfies the given where condition
     * @param schema schema of table containing record
     * @param record record to check
     * @param input where clause to check
     * @param fromTablesSchemas overhead for where functions
     * @return true or false, whether where clause is satisfied or not
     * @throws Exception
     */
    public static boolean whereRecord(TableSchema schema, Record record, String input, List<TableSchema> fromTablesSchemas) throws Exception{

        WhereNode root = parseWhere(tokenizeWhere(input));

        Map<String, Object> row = new HashMap<>();

        List<Object> recordValues = DMLParser.convert_record_to_values(schema.getAttributes(), record);

        for(int i = 0; i < schema.getAttributes().size(); i++){

            row.put((schema.getAttributes().get(i).getName()), recordValues.get(i));

        }

        return evaluateCondition(root, row, fromTablesSchemas.get(0).getTableName(), fromTablesSchemas.size());

    }

    /**
     * main where function; filters given table(s) with given where clause to return new table schema
     * @param schema given schema to check where clause against
     * @param input given input string of where clause
     * @param catalog working catalog
     * @param storageManager working storage manager
     * @param fromTablesSchemas overhead for where functions
     * @return resulting table schema
     * @throws Exception any input errors for the where clause
     */
    /*TODO: Do a full analysis of where and rework it so that it actually works */
    public static TableSchema where(TableSchema schema, String input, Catalog catalog, StorageManager storageManager, List<TableSchema> fromTablesSchemas) throws Exception {

        if(input.equals("")){

            return schema;

        }

        WhereNode root = parseWhere(tokenizeWhere(input));
        TableSchema temp = new TableSchema("where_temp" + catalog.getLastUsed() + 1, catalog.getLastUsed() + 1);

        for(AttributeSchema attr : schema.getAttributes()){

            String newName = attr.getName();
            AttributeSchema current = new AttributeSchema(newName, attr.getType(), attr.getSize(), false, false, false);
            temp.addAttribute(current);

        }

        catalog.addTable(temp, false);

        //List<Integer> page_nums = storageManager.getPageOrder(schema.getTableNum(), false);
        List<Integer> page_nums = Catalog.getTablePageOrder(schema.getTableNum());
        // loop through all pages
        for(Integer page_num : page_nums){

            Page cur_page = storageManager.getPage(schema.getTableNum(), page_num);
            List<Record> page_records = cur_page.getRecords();
            
            for(Record record : page_records){
                
                Map<String, Object> row = new HashMap<>();
                List<Object> recordValues = DMLParser.convert_record_to_values(schema.getAttributes(), record);

                for(int i = 0; i < schema.getAttributes().size(); i++){

                    row.put((schema.getAttributes().get(i).getName()), recordValues.get(i));

                }

                try{

                    if(evaluateCondition(root, row, fromTablesSchemas.get(0).getTableName(), fromTablesSchemas.size())){

                        //List<Integer> oldOrder = storageManager.getPageOrder(temp.getTableNum(), false);
                        storageManager.insertRecord(record, temp.getTableNum(), true, false);
    
                    }

                } catch (Exception e){

                    catalog.removeTableByName(temp.getTableName());
                    throw e;

                }

            }

        }

        return temp;

    }

    /**
     * helper function for where; checks if a string value is a valid non-logical operator
     * @param val value to check
     * @return true or false
     */
    private static boolean isValidOperator(String val){

        return val.equals("=") || val.equals("!=") || val.equals(">") || 
               val.equals("<") || val.equals(">=") || val.equals("<=");

    }
    
    /**
     * checks if a string value is a valid logical operator
     * @param val value to check
     * @return true or false
     */
    private static boolean isLogicalOperator(String val){

        return val.equalsIgnoreCase("AND") || val.equalsIgnoreCase("OR");

    }
    
    /**
     * detects any early errors with the given where tree, throws errors
     * @param node root node of tree
     */
    private static void validateWhereTree(WhereNode node){

        if(node == null){

            throw new IllegalArgumentException("Invalid condition: Empty WHERE clause.");

        }
    
        // If the node is an operator, it must have both left and right children
        if(isLogicalOperator(node.val) || isValidOperator(node.val)){

            if(node.left == null || node.right == null){

                throw new IllegalArgumentException("Invalid condition: Operator '" + node.val + "' requires two operands.");

            }
            // Recursively validate left and right
            validateWhereTree(node.left);
            validateWhereTree(node.right);

        }else{
            // If it's not an operator, it should be a leaf (column name or value)
            if (node.left != null || node.right != null) {

                throw new IllegalArgumentException("Invalid condition: '" + node.val + "' should not have children.");

            }

        }

    }
    

    /**
     * main where logic, goes through the tree and evaluates the where clause
     * @param node root node of parsed where tree
     * @param row row to evaluate; key is name of the attribute (string), value is the row's value for that attribute
     * @param tableName name of working table
     * @param fromTablesSize overhead for helper functions
     * @return true or false, if the row meets the where clause 
     */
    public static boolean evaluateCondition(WhereNode node, Map<String, Object> row, String tableName, int fromTablesSize){
        
        validateWhereTree(node);

        if(node == null) return false;
        
        /*if(node.left == null && node.right == null){

            if(row.containsKey(node.val)){

                Object value = row.get(node.val);

                if(value instanceof Boolean){

                    return (Boolean) value;

                }

                return false; 

            }

            return isBooleanLiteral(node.val); 

        }*/

        if (isOperator(node.val)){

            Object leftVal = getValue(node.left, row, tableName, fromTablesSize);
            Object rightVal = getValue(node.right, row, tableName, fromTablesSize);

            if (leftVal == null || rightVal == null) {
                throw new IllegalArgumentException("Invalid WHERE clause: Operand is missing or undefined.");
            }

            if (leftVal instanceof Boolean && rightVal instanceof Boolean){

                if (!node.val.equals("=") && !node.val.equals("!=")){

                    throw new IllegalArgumentException("Boolean values can only be compared with '=' or '!='.");

                }

                return node.val.equals("=") ? leftVal.equals(rightVal) : !leftVal.equals(rightVal);

            }

            return switch(node.val){

                case "=" -> equalTo(leftVal, rightVal);
                case "!=" -> !equalTo(leftVal, rightVal);
                case ">" -> compare(leftVal, rightVal) > 0;
                case "<" -> compare(leftVal, rightVal) < 0;
                case ">=" -> compare(leftVal, rightVal) >= 0;
                case "<=" -> compare(leftVal, rightVal) <= 0;
                case "and" -> evaluateCondition(node.left, row, tableName, fromTablesSize) && evaluateCondition(node.right, row, tableName, fromTablesSize);
                case "or" -> evaluateCondition(node.left, row, tableName, fromTablesSize) || evaluateCondition(node.right, row, tableName, fromTablesSize);
                default -> throw new IllegalArgumentException("Invalid operator");
    
            };

        }

        throw new IllegalArgumentException("Unexpected argument: " + node.val);

    }

    /**
     * helper function for where; checks the row table to see if the value is from the table
     * @param node node to check value of
     * @param row map of row to check value from
     * @param tableName name of working table
     * @param fromTablesSize used to check if there is only 1 working table, if so "foo.x" and "x" are both possible
     * @return the resulting value, whether from the row or just as a literal
     */
    private static Object getValue(WhereNode node, Map<String, Object> row, String tableName, int fromTablesSize){

        if(fromTablesSize == 1 && row.containsKey(tableName + "." + node.val)){

            return row.get(tableName + "." + node.val);

        }else if(row.containsKey(node.val)){

            return row.get(node.val);
            
        }

        return parseLiteral(node.val); 

    }

    /**
     * helper function for where; parses a literal when an operand is not from the row
     * @param value value to parse
     * @return resulting parsed literal
     */
    private static Object parseLiteral(String value){

        if (value.equalsIgnoreCase("true")) return "true";
        if (value.equalsIgnoreCase("false")) return "false";
        //will handle a value being null; can't return actual null because will cause problems
        if (value.equals("null")) return "null";

        try{

            return Integer.parseInt(value);  

        }catch(NumberFormatException ignored){}

        try{

            return Double.parseDouble(value); 

        } catch (NumberFormatException ignored) {}

        if(value.startsWith("\"") && value.endsWith("\"")){

            return value.substring(1, value.length() - 1); 

        }

        /*TODO: If my logic is correct, the only things that should be making it here 
         *          if value is either an operator or a attribute name that is not in the table
         *  Probably need to check if it is part of the valid operators,
         *      If so return the value,
         *      If not throw an error
        */

        if (isOperator(value)){
            return value;
        } else {
            throw new IllegalArgumentException("The table does not have an attribute with the name '"+value+"'");
        }

        //return value;

    }


    /**
     * helper function for where; checks if the given value is a boolean literal
     * @param value value to check
     * @return true or false
     */
    private static boolean isBooleanLiteral(String value){

        return value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false");

    }

    /**
     * helper function for where; compares two given values (<, >, >=, <=)
     * @param a left value to compare
     * @param b right value to compare
     * @return the result which will be greater than or less than zero
     */
    private static int compare(Object a, Object b){

        if(a instanceof Number && b instanceof Number){

            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());

        }else if(a instanceof String && b instanceof String){

            return ((String) a).compareTo((String) b);

        }

        throw new IllegalArgumentException("Invalid comparison: " + a + " and " + b);

    }

    /*
     * TODO: NOTE: Be aware that because there is no actual type checking of attributes,
     *       if two null values are compared, the where will return true
     *      NOTHING THAT CAN BE DONE ABOUT THIS!!!!!!
    */
    /**
     * helper function for where; checks if two given values are equal to each other
     * @param a left value
     * @param b right value
     * @return true or false
     */
    private static boolean equalTo(Object a, Object b){
        
        //a lot of the else if checks to see if one of the objects is
        //  the null string
        if (a instanceof Integer && b instanceof Integer){
            return ((Integer) a).equals( (Integer) b);
        }else if (a instanceof Integer && b instanceof String){
            if (((String)b).equals("null")) return false;
            throw new IllegalArgumentException("Invalid comparison: " + a + " and \"" + b+"\"");
        }else if (a instanceof String && b instanceof Integer){
            if (((String)a).equals("null")) return false;
            throw new IllegalArgumentException("Invalid comparison: \"" + a + "\" and " + b);
        }else if(a instanceof Double && b instanceof Double){
            return ((Double) a).equals( (Double) b);
        }else if(a instanceof Double && b instanceof String){
            if (((String)b).equals("null")) return false;
            throw new IllegalArgumentException("Invalid comparison: " + a + " and \"" + b+"\"");
        }else if(a instanceof String && b instanceof Double){
            if (((String)a).equals("null")) return false;
            throw new IllegalArgumentException("Invalid comparison: \"" + a + "\" and " + b);
        }else if(a instanceof String && b instanceof String){
            //does boolean check because we convert booleans to string value of true and false
            //also does null check because null is a string, not actually null
            if (((String)a).equals("true")){
                if (((String)b).equals("true")){
                    return true;
                } else if (((String)b).equals("false")){
                    return false;
                } else if (((String)b).equals("null")){
                    return false;
                }
                throw new IllegalArgumentException("Invalid comparison: " + a + " and \"" + b+"\"");
            } else if (((String)a).equals("false")){
                if (((String)b).equals("false")){
                    return true;
                }else if (((String)b).equals("true")){
                    return false;
                }else if (((String)b).equals("null")){
                    return false;
                }
                throw new IllegalArgumentException("Invalid comparison: " + a + " and \"" + b+"\"");
            } else if (((String)a).equals("null")){
                if (((String)b).equals("null")){
                    return true;
                }else if (((String)b).equals("true")){
                    return false;
                }else if (((String)b).equals("false")){
                    return false;
                }
            }

            if (((String)b).equals("true") || ((String)b).equals("false")){
                throw new IllegalArgumentException("Invalid comparison: \"" + a + "\" and " + b);
            }
            //does regular string comparison
            return ((String) a).equals((String) b);

        }

        throw new IllegalArgumentException("Invalid comparison: " + a + " and " + b);

    }


    /**
     * Creates a new table containing only the columns specified in 'requestedColumns'
     * from the given 'oldSchema' table. It does NOT drop columns from the old table;
     * it builds a fresh table with the subset of attributes.
     *
     * @param catalog          the Catalog of the database
     * @param storageManager   the StorageManager for records/pages
     * @param oldSchema        the original (temporary/combined) table schema to project from
     * @param requestedColumns the list of columns we want to keep. Can be "tableName.col" or just "col".
     *                         If exactly one entry is "*", keep all columns.
     * @return                 a new TableSchema with only the selected columns and their data
     */
    public static TableSchema select_select(
        Catalog catalog,
        StorageManager storageManager,
        TableSchema oldSchema,
        String[] requestedColumns
    ) throws Exception 
    {
        boolean keepAll = requestedColumns != null && requestedColumns.length == 1 && requestedColumns[0].equals("*");

        // Step 1: Map short names and full names to their indexes
        List<AttributeSchema> oldAttrs = oldSchema.getAttributes();
        Map<String, Integer> fullNameToIndex = new HashMap<>();
        Map<String, List<Integer>> shortNameToIndices = new HashMap<>();

        for (int i = 0; i < oldAttrs.size(); i++) {
            String fullName = oldAttrs.get(i).getName(); // e.g. "foo.x"
            String shortName = fullName.contains(".") ? fullName.substring(fullName.indexOf('.') + 1) : fullName;

            fullNameToIndex.put(fullName, i);
            shortNameToIndices.computeIfAbsent(shortName, k -> new ArrayList<>()).add(i);
        }

        // Step 2: Create new schema and list of indices to keep in correct order
        int newTableNum = catalog.getLastUsed() + 1;
        TableSchema newSchema = new TableSchema("select_temp" + newTableNum, newTableNum);
        List<Integer> keepIndices = new ArrayList<>();

        if (keepAll) {
            for (int i = 0; i < oldAttrs.size(); i++) {
                keepIndices.add(i);
                newSchema.addAttribute(new AttributeSchema(
                    oldAttrs.get(i).getName(),
                    oldAttrs.get(i).getType(),
                    oldAttrs.get(i).getSize(),
                    oldAttrs.get(i).isPrimaryKey(),
                    false,
                    false
                ));
            }
        } else {
            for (String col : requestedColumns) {
                Integer index = null;
                AttributeSchema matchedAttr = null;

                // Case 1: full name
                if (fullNameToIndex.containsKey(col)) {
                    index = fullNameToIndex.get(col);
                    matchedAttr = oldAttrs.get(index);
                }
                // Case 2: short name, check ambiguity
                else if (shortNameToIndices.containsKey(col)) {
                    List<Integer> indices = shortNameToIndices.get(col);
                    if (indices.size() > 1) {
                        throw new IllegalArgumentException("Ambiguous column name: '" + col + "'. Please disambiguate using 'table.column'.");
                    }
                    index = indices.get(0);
                    matchedAttr = oldAttrs.get(index);
                } else {
                    throw new IllegalArgumentException("Attribute '" + col + "' does not exist in the schema.");
                }

                // Add attribute and remember index in requested order
                newSchema.addAttribute(new AttributeSchema(
                    matchedAttr.getName(),
                    matchedAttr.getType(),
                    matchedAttr.getSize(),
                    matchedAttr.isPrimaryKey(),
                    false,
                    false
                ));
                keepIndices.add(index);
            }
        }

        if (newSchema.getAttributes().isEmpty()) {
            catalog.addTable(newSchema, false);
            return newSchema;
        }

        catalog.addTable(newSchema, false);

        int oldTableNum = oldSchema.getTableNum();
        //List<Integer> oldPageNums = storageManager.getPageOrder(oldTableNum, false);
        List<Integer> oldPageNums = Catalog.getTablePageOrder(oldTableNum);

        for (Integer oldPage : oldPageNums) {
            Page page = storageManager.getPage(oldTableNum, oldPage);
            List<Record> records = page.getRecords();

            for (Record oldRecord : records) {
                ArrayList<Object> allValues = DMLParser.convert_record_to_values(oldAttrs, oldRecord);

                ArrayList<Object> projectedValues = new ArrayList<>();
                List<AttributeType> projectedTypes = new ArrayList<>();

                for (int idx : keepIndices) {
                    projectedValues.add(allValues.get(idx));
                    projectedTypes.add(oldAttrs.get(idx).getType());
                }

                byte[] newData = DMLParser.convert_values_to_record(projectedTypes, projectedTypes, projectedValues);
                Record newRec = new Record(newData);
                //List<Integer> newPageOrder = storageManager.getPageOrder(newSchema.getTableNum(), false);
                storageManager.insertRecord(newRec, newSchema.getTableNum(), true, false);
            }
        }

        return newSchema;
    }



    /**
     * performs the "orderby" function of a select statement. orders the table by the given argument
     * @param catalog catalog of database to use
     * @param storage_manager storage manager of database to use
     * @param table the table schema to filter
     * @param argument the argument to order the table by
     * @return new table schema of the filtered table
     * @throws IOException
     */
    public static TableSchema select_orderby(Catalog catalog, StorageManager storage_manager, TableSchema table, String argument) throws Exception{
        // create temporary table
        TableSchema orderby_table = new TableSchema("orderby_temp" + catalog.getLastUsed() + 1, catalog.getLastUsed() + 1);
        List<TableSchema> to_delete = new ArrayList<>();
        to_delete.add(orderby_table);
        List<String> attr_names = new ArrayList<>(); // list of all attribute names
        List<String> attr_names_split = new ArrayList<>(); // list of all attribute names split by "."


        // go through all attributes, renaming with prefix "{tableName}." before each attribute name and add to new table schema
        for(AttributeSchema attr : table.getAttributes()){
            String new_attr_name = attr.getName(); // get the attribute name
            attr_names.add(new_attr_name);

            String[] split_attr_name = new_attr_name.split("\\."); // split string array to get the attribute name
            attr_names_split.add(split_attr_name[1]);
    
            // check for ambiguous attributes
            for (int i = 0; i < attr_names_split.size(); i++) {
                for (int j = i + 1; j < attr_names_split.size(); j++) {
                    if (attr_names_split.get(i).equals(attr_names_split.get(j)) && argument.equals(attr_names_split.get(i))) {
                        System.err.println(attr_names_split.get(i).toUpperCase() + " is ambiguous");
                        throw new IllegalArgumentException("Ambiguous attribute");
                    }
                }
            }

            AttributeSchema cur_attr = null;
            if(new_attr_name.equals(argument) || new_attr_name.contains("." + argument)){ 
                cur_attr = new AttributeSchema(new_attr_name, attr.getType(), attr.getSize(), true, false, false);
            }
            else{
                cur_attr = new AttributeSchema(new_attr_name, attr.getType(), attr.getSize(), false, false, false);
            }
            orderby_table.addAttribute(cur_attr);
            
        }

        //checks if the attribute to order by is in the table
        for(String attr_name : attr_names){
            if(attr_name.equals(argument) || attr_name.contains("." + argument)){
                break;
            }
            if(attr_name.equals(attr_names.get(attr_names.size() - 1))){ // if the attribute is not in the table by the end of the loop
                System.err.println("Attribute: " + argument + " not in table");
                throw new IllegalArgumentException("Attribute not in table");
            }
        }

        catalog.addTable(orderby_table, false);
        //List<Integer> page_nums = storage_manager.getPageOrder(table.getTableNum(), false);
        List<Integer> page_nums = Catalog.getTablePageOrder(table.getTableNum());
        // loop through all pages
        for(Integer page_num : page_nums){
            Page cur_page = storage_manager.getPage(table.getTableNum(), page_num);
            List<Record> page_records = cur_page.getRecords();
            // copy over all records from previous table
            for(Record record : page_records){
                //List<Integer> new_table_page_nums = storage_manager.getPageOrder(orderby_table.getTableNum(), false);
                storage_manager.insertRecord(record, orderby_table.getTableNum(), false, false);
            }
        }
        
        //delete_temp_table(catalog, to_delete);
        catalog.saveCatalog();

        return orderby_table;
    }

    /*public static void delete_temp_table(Catalog catalog, List<TableSchema> to_delete) throws IOException{
        for(int j = 0; j < to_delete.size(); j++){
            System.out.println("Deleting table: " + to_delete.get(j).getTableName()); // delete the temporary table
            String table_name = to_delete.get(j).getTableName();
            catalog.removeTableByName(table_name);
        }
    }*/
    
    /**
    /**
     * deletes all records from a given table that satisfy the given where condtions
     * @param catalog catalog of the database
     * @param storage_manager storage manager of the database
     * @param where_arguments conditions to be satisfied to delete
     * @throws IOException 
     */
    public static void delete(Catalog catalog, StorageManager storage_manager, String table_name, String where_arguments) throws Exception{
        TableSchema original_schema = catalog.getTableSchemaByName(table_name);
        // duplicates the schema
        TableSchema duplicate_schema = catalog.duplicateSchema(table_name);

        // deletes all rows that satisfy the where condition
        storage_manager.deleteRows(where_arguments, original_schema.getTableNum());

        // updates the catalog with new table
        catalog.updateTableSchema(original_schema, duplicate_schema);
        catalog.saveCatalog();
    }


    /**
     * Checks the validity of all the parts of the update statement
     * @param catalog the catalog for this database
     * @param table_name the name of the table where records will be updated
     * @param columnName the name of the attribute where a records value will be updated
     * @param value the value to update the record with
     * @param valueIsString boolean for making sure that strings are entered correctly
     * @param whereCaluse the where clause to check each record with
     * @param storage_manager the storage manager for this database
     * @throws IOException
     */
    public static void update(Catalog catalog, String table_name, String columnName, String value, boolean valueIsString, String whereClause, StorageManager storage_manager) throws IOException{
        TableSchema tableSchema = catalog.getTableSchemaByName(table_name);
        //check if the table with the given table name exists
        if (tableSchema == null){
            System.err.println("\nTabel with name '"+table_name+"' does not exist\nERROR");
            return;
        }

        //check if the attribute with the given attribute name exists in the table
        List<AttributeSchema> attributeList = tableSchema.getAttributes();
        AttributeSchema column = null;
        int attributeIndex = 0;
        for (AttributeSchema attribute : attributeList){
            if (attribute.getName().equals(columnName)){
                column = attribute;
                break;
            }
            attributeIndex += 1;
        }
        if (column == null){
            System.err.println("\nTable '"+table_name+"' does not contain attribute '"+columnName+"'\nERROR");
            return;
        }

        //Do type checking: Compare the type of the attribute with the value given
        AttributeType columnType = column.getType();
        //if value is 'null' change it to actually equal null
        if(value.equals("null")){
            if (valueIsString){
                System.err.println("\nAmbigious Input: if you want to enter the word 'null' as"+
                                        " a String, please make at least one of the letter nonlowercase\nERROR");
                return;     
            }
            if (column.isNotNull()){
                System.err.println("\nInvalid update: "+columnName+" is notNull\nERROR");
                return;
            }
            value = null;
        //make sure that a boolean has a value of either true or false
        }else if (columnType == AttributeType.BOOLEAN){
            if (!(value.toLowerCase().equals("true") ||
                value.toLowerCase().equals("false"))){
                System.err.println("\nType Missmatch: '"+value+"' can not be assigned to attribute '"+columnName+"' of type Boolean\nERROR");
                return;
            }
        //make sure the value is a double if it should be a double
        } else if(columnType == AttributeType.DOUBLE){
            if (!value.contains(".")){
                System.err.println("\nType Missmatch: '"+value+"' can not be assigned to attribute '"+columnName+"' of type Double\nERROR");
                return;
            }
            try{
                Double.valueOf(value);
            } catch (NumberFormatException e){
                System.err.println("\nType Missmatch: '"+value+"' can not be assigned to attribute '"+columnName+"' of type Double\nERROR");
                return;
            }
        //make sure the value is an integer if it should be a integer
        } else if(columnType == AttributeType.INTEGER){
            try{
                Integer.valueOf(value);
            } catch (NumberFormatException e){
                System.err.println("\nType Missmatch: '"+value+"' can not be assigned to attribute '"+columnName+"' of type Integer\nERROR");
                return;
            }
        //make sure the value is a character if it should be a character
        //needs to be in the propper string format
        //needs to be the exact size of the char for this attribute
        } else if (columnType == AttributeType.CHAR){
            if (!valueIsString){
                System.err.println("\nImproper String Format: value of char must be in format \"<value>\"\nERROR");
                return;
            }
            int charSize = column.getSize();
            if (value.length() != charSize){
                System.err.println("\nImproper Char Size: values of attribute '"+columnName+"' must be "+charSize+" characters in length\nERROR");
                return;
            }
        //make sure the value is a varchar if it should be a varchar
        //needs to be in the propper string format
        //needs to be eqaul to or less than the max size of a varchar for this attribute
        } else if (columnType == AttributeType.VARCHAR){
            if (!valueIsString){
                System.err.println("\nImproper String Format: value of varchar must be in format \"<value>\"\nERROR");
                return;
            }
            int maxVarCharSize = column.getSize();
            if (value.length() > maxVarCharSize){
                System.err.println("\nImproper VarChar Size: values of attribute '"+columnName+"' must be less than or equal to "+maxVarCharSize+" characters in length\nERROR");
                return;
            }
        }

        int tableId = tableSchema.getTableNum();
        boolean isUnique = false; //attribute for telling update that only one record should be updated, if any can be

        //Checks to see if given the attribute having the unique constraint,
        //  if there is already a record with the given value
        if (value != null && column.isUnique()){
            if (storage_manager.checkForSameValue(tableId, tableSchema, attributeIndex, value)){
                System.err.println("\nInvalid Update: "+columnName+" is unique and record already has value of "+value+"\nERROR");
                return;
            }

            isUnique = true;
        }


        if (column.isPrimaryKey()){
            //Can not set the primaryKey value to null
            if (value == null){
                System.err.println("\nInvalid Update: can not set PrimaryKey value to null\nERROR");
                return;
            }

            //Checks to see if given the attribute being the primaryKey,
            //  if there is already a record with the given value
            if (storage_manager.checkForSameValue(tableId, tableSchema, attributeIndex, value)){
                System.err.println("\nInvalid Update: "+columnName+" is primaryKey and record already has value of "+value+"\nERROR");
                return;
            }

            isUnique = true;
        }
        
        //creates a new TableSchema object that is a duplicate of another one
        TableSchema copyTableSchema = catalog.duplicateSchema(table_name);

        //System.out.println("Even Spicier of the meatballs");
        System.out.println();
        try{
            //update some records in the table
            storage_manager.updateRecords(tableId, copyTableSchema, attributeIndex, value, isUnique, whereClause);
        } catch (Exception e){
            //updates the catalog so that old TableSchema is removed and new TableSchema is added
            catalog.updateTableSchema(tableSchema, copyTableSchema);
            //saves the catalog
            catalog.saveCatalog();
            System.err.println(e.getMessage()+"\nERROR");
            //e.printStackTrace();
            return;
        }
        
        //updates the catalog so that old TableSchema is removed and new TableSchema is added
        catalog.updateTableSchema(tableSchema, copyTableSchema);
        //saves the catalog
        catalog.saveCatalog();
        System.out.println("SUCCESS");
    }
}
