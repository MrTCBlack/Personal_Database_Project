/**
 * Contains all the parser functions used to parse user given SQL statements
 * 
 * @author Teagan Harvey tph6529
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLParser {
    /**
     * Parse an insert statement from the user in the form:
     *      insert into <name> values <tuples>;
     * By Passing the table name and list of tuples
     * 
     * @param input - statement to be parsed
          * @throws IOException 
          */
         public static void parse_insert(Catalog catalog, String input, StorageManager storage_manager) throws IOException{
        // check formating
        String[] input_split = input.substring(0, input.length() - 1).replace(", ", ",").split(" ", 5);
        if(input_split.length == 5 && input_split[1].toLowerCase().equals("into") &&  input_split[3].toLowerCase().equals("values")){
            // get table name and values to be inserted
            String table_name = input_split[2];
            String[] raw_values = input_split[4].split(",");

            ArrayList<List<String>> values = new ArrayList<List<String>>();

            // seperates all tuples and saves them
            for(String tuple : raw_values){
                if(tuple.startsWith("(") && tuple.endsWith(")")){
                    tuple = tuple.substring(1, tuple.length() - 1);
                        
                    // splits string on space not including if its surrounded by quotes
                    List<String> tuple_values = new ArrayList<String>();
                    Pattern regex = Pattern.compile("[^\s\"']+|\"[^\"]*\"|'[^']*'");
                    Matcher regexMatcher = regex.matcher(tuple);
                    while (regexMatcher.find()) {
                        tuple_values.add(regexMatcher.group());
                    } 
                        
                values.add(tuple_values);
                }else{
                    System.out.println("Invalid Insert Statement!");
                    return;
                }
            }
            DMLParser.insert(catalog, table_name, values, storage_manager);
        }else{
            System.out.println("Invalid Insert Statement!");
        }
    }

    /**
     * Parse a select statement from the user in the form:
     *      select <attributes> from <name> [where <conditions>] [orderby <attribute>];
     * where and orderby are optional
     * @param catalog catalog of the database
     * @param input input to parse
     * @param storage_manager storage manager of the database
     * @throws IOException
     */
    public static void parse_select(Catalog catalog, String input, StorageManager storage_manager) throws Exception{
        // if no arguments given at all, ERROR
        if(input.length() < 8){
            System.out.println("Invalid Select Statement!");
            return;
        }
        input = input.substring(7, input.length() - 1);

        int operation = 0;
        String arguments = "";
        String cur_word = "";
        String select_arguments = "";
        String from_arguments = "";
        String where_arguments = "";
        String orderby_arguments = "";
        // loop over entire select statement
        for(int i = 0; i < input.length(); i++){
            char cur_char = input.charAt(i);
            cur_word += cur_char;
            // if letter is a space or end of arguments, assume it is a word
            if(cur_char == ' ' || i == (input.length() - 1)){
                String formated_word = cur_word.toLowerCase().strip();
                // see if word is any keyword
                if(formated_word.equals("from") || formated_word.equals("where") || formated_word.equals("orderby")){
                    // if no arguments given after keyword, ERROR
                    if(arguments.equals("") || i == (input.length() - 1)){
                        System.out.println("Invalid Select Statement!");
                        return;
                    }
                    // add to specific set of arguemnts based on given keyword
                    String formated_arguments = arguments.substring(0, arguments.length() - 1);
                    if(formated_word.equals("from")){
                        select_arguments = formated_arguments;
                    }else if(formated_word.equals("where")){
                        from_arguments = formated_arguments;
                    }else if (formated_word.equals("orderby")){
                        if(from_arguments.equals("")){
                            from_arguments = formated_arguments;
                            operation += 1;
                        }else{
                            where_arguments = formated_arguments;
                        }
                    }
                    operation += 1;
                    arguments = "";
                }else{
                    arguments += cur_word;
                }
                cur_word = "";
            }
        }
        arguments += cur_word;

        // add arguments based on last given keyword
        if(operation == 1){
            from_arguments = arguments;
        }else if(operation == 2){
            where_arguments = arguments;
        }else{
            orderby_arguments = arguments;
        }

        // if select or from arguments empty, ERROR
        if(select_arguments.equals("") || from_arguments.equals("")){
            System.out.println("Invalid Select Statement!");
            return;
        }

        // split select and from arguments
        String[] select_arguments_split = select_arguments.split(", ");
        String[] from_arguments_split = from_arguments.replaceAll(", ", ",").split(",");

        DMLParser.select(catalog, storage_manager, select_arguments_split, from_arguments_split, where_arguments, orderby_arguments);
    }

    /**
     * Parse a display info statement from the user in the form:
     *      display info <name>;
     * By passing the table name
     * 
     * @param input - statement to be parsed
     * @throws IOException 
     */
    public static void parse_display_info(Catalog catalog, String input, StorageManager storage_manager) throws IOException{
        // check format
        String[] input_split = input.substring(0, input.length() - 1).split(" ");
        if(input_split.length == 3){
            String table_name = input_split[2];
            DMLParser.display_info(catalog, table_name, storage_manager);
        }else{
            System.out.println("Invalid Display Statement");
        }
    }

    /**
     * Parse a delete statement from the user in the form:
     *      delete from <name> [where <conditions>];
     * where is optional.
     * @param catalog catalog of database
     * @param input input to be parsed
     * @param storage_manager storage manager of the database
     * @throws IOException
     */
    public static void parse_delete(Catalog catalog, String input, StorageManager storage_manager) throws Exception{
        // check format
        String[] input_split = input.substring(0, input.length() - 1).split(" ", 5);

        String table_name = "";
        String where_arguments = "";
        // parse if includes where
        if(input_split.length == 5 && input_split[1].toLowerCase().equals("from") && input_split[3].toLowerCase().equals("where")){
            table_name = input_split[2];
            String old_where_arguments = input_split[4];
            String[] where_arguments_split = old_where_arguments.split(" ");
            int length_to_split = table_name.length() + 1;

            // take off table name from attributes
            for(int i = 0; i < where_arguments_split.length; i++){
                String unit = where_arguments_split[i];
                if(unit.startsWith(table_name + ".")){
                    where_arguments += unit.substring(length_to_split, unit.length());
                }else{
                    where_arguments += unit;
                }
                if(i < where_arguments_split.length - 1){
                    where_arguments += " ";
                }
            }

            DMLParser.delete(catalog, table_name, where_arguments, storage_manager);
        // parse if doesn't include where
        }else if(input_split.length == 3 && input_split[1].toLowerCase().equals("from")){
            table_name = input_split[2];
            DMLParser.delete(catalog, table_name, where_arguments, storage_manager);
        // ERROR otherwsie
        }else{
            System.out.println("Invalid Delete Statement");
        }
    }

    /**
     * parses user input for the DDL parser
     * @param input the user's command statement
     * @param catalog the catalog of the database
     * @throws Exception
     */
    public static void parseTable(String input, Catalog catalog, StorageManager storageManager) throws Exception{

        DDLParser parser = new DDLParser(input, catalog, storageManager);
        parser.parse();

    }

    /**
     * Parse a update statement from the user in the form:
     *      update < name >
     *      set < column_1 > = < value >
     *      where < condition >;
     * @param catalog the catalog for this database
     * @param input the user's command statement
     * @param storage_manager the storage manager for this database
     * @throws IOException
     */
    public static void parse_update(Catalog catalog, String input, StorageManager storage_manager) throws IOException{
        String[] input_split = input.split(" ");

        // if no arguments given at all, ERROR
        if(input_split.length <= 1){
            System.err.println("\nInvalid Update Statement!\nERROR");
            return;
        }

        int currentIndex = 1;
        String tableName = input_split[currentIndex];
        currentIndex += 1;

        //update statement does not include set
        if (input_split.length-1 < currentIndex){
            System.err.println("\nInvalid Update Statement!\nERROR");
            return;
        //current input needs to be 'set'
        }else if (!(input_split[currentIndex].toLowerCase()).equals("set")){
            System.err.println("\nInvalid Update Statement!\nERROR");
            return;
        }
        currentIndex += 1;

        //update statement does not include column name
        if (input_split.length-1 < currentIndex){
            System.err.println("\nInvalid Update Statement!\nERROR");
            return;
        }
        String attributeName = input_split[currentIndex];
        //System.out.println(attributeName);
        if (attributeName.startsWith(tableName+".")){
            attributeName = attributeName.substring(tableName.length()+1, attributeName.length());
        }
        //System.out.println(attributeName);
        currentIndex += 1;

        //update statement does not include '='
        if (input_split.length-1 < currentIndex){
            System.err.println("\nInvalid Update Statement!\nERROR");
            return;
        //current input needs to be '='
        } else if(!input_split[currentIndex].equals("=")){
            System.err.println("\nInvalid Update Statement!\nERROR");
            return;
        }
        currentIndex += 1;

        //update statement does not include update value
        if (input_split.length-1 < currentIndex){
            System.err.println("\nInvalid Update Statement!\nERROR");
            return;
        }
        String updateValue = input_split[currentIndex];
        boolean valueIsString = false;
        if(updateValue.charAt(updateValue.length()-1) == ';'){
            updateValue = updateValue.substring(0, updateValue.length()-1);
        }
        if (updateValue.charAt(0)=='"'){
            valueIsString = true;
            updateValue = updateValue.substring(1);
            if (updateValue.charAt(updateValue.length()-1) == '"'){
                updateValue = updateValue.substring(0, updateValue.length()-1);
            } else if(updateValue.charAt(updateValue.length()-1) == ';' &&
                        updateValue.charAt(updateValue.length()-2) == '"'){
                updateValue = updateValue.substring(0, updateValue.length()-2);
            } else {
                updateValue += " ";
                currentIndex += 1;
                if (input_split.length-1 < currentIndex){
                    System.err.println("\nImproper input for value of Char or Varchar!\nERROR");
                    return;
                }
                String nextPartOfString = input_split[currentIndex];
                while (nextPartOfString.charAt(nextPartOfString.length()-1) != '"'){
                    if (nextPartOfString.charAt(nextPartOfString.length()-1) == ';' &&
                        nextPartOfString.charAt(nextPartOfString.length()-2) == '"'){
                        break;
                    }
                    updateValue += nextPartOfString;
                    updateValue += " ";
                    currentIndex += 1;
                    if (input_split.length-1 < currentIndex){
                        System.err.println("\nImproper input for value of Char or Varchar!\nERROR");
                        return;
                    }
                    nextPartOfString = input_split[currentIndex];
                }
                updateValue += nextPartOfString;
                if (updateValue.charAt(updateValue.length()-1) == '"'){
                    updateValue = updateValue.substring(0, updateValue.length()-1);
                } else if(updateValue.charAt(updateValue.length()-1) == ';' &&
                            updateValue.charAt(updateValue.length()-2) == '"'){
                    updateValue = updateValue.substring(0, updateValue.length()-2);
                }
            }
        }
        currentIndex += 1;
        
        //boolean whereBoolean = false;
        String whereCondition;
        //update statement does not include where clause
        //  All tuples will be updated
        if (input_split.length-1 < currentIndex){
            whereCondition = "";
        } else if (input_split[currentIndex].equals(";")){
            whereCondition = "";
        }else {
            //current input needs to be 'where'
            if(!(input_split[currentIndex].toLowerCase()).equals("where")){
                System.err.println("\nInvalid Update Statement!\nERROR");
                return;
            }
            currentIndex += 1;
            
            //Nothing following the where clause
            if (input_split.length-1 < currentIndex){
                System.err.println("\nInvalid Update Statement!\nERROR");
                return;
            }

            whereCondition = "";
            while(currentIndex < input_split.length-1){
                String token = input_split[currentIndex];
                //System.out.println(token);
                if (token.startsWith(tableName+".")){
                    token = token.substring(tableName.length()+1, token.length());
                }
                //System.out.println(token);
                whereCondition += token;
                whereCondition += " ";
                currentIndex += 1;
            }
            String lastInput = "";
            char[] charArr = input_split[currentIndex].toCharArray();
            for (int i = 0; i < charArr.length-1; i++){
                lastInput += charArr[i];
            }
            //System.out.println(lastInput);
            if (lastInput.startsWith(tableName+".")){
                lastInput = lastInput.substring(tableName.length()+1, lastInput.length());
            }
            //System.out.println(lastInput);
            whereCondition += lastInput;
            //System.out.println(whereCondition);
            //whereBoolean = true;
        }

        //System.out.println(updateValue);
        //System.out.println("That's a spice meatball");
        DMLFunctions.update(catalog, tableName, attributeName, updateValue, valueIsString, whereCondition, storage_manager);
    }
}
