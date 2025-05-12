/**
 * Contains main function of program representing the implementation of a database
 * 
 * @author Teagan Harvey tph6529
 * @author Tyler Black tcb8683
 */

import java.util.Scanner;
import java.io.File;

/**
 * Main function of program; checks for db; if not present, creates one; loops over user input
 * to enter SQL statements and interact with the DB
 */
public class Main {
    static String catalogFileName = "catalog";
    static String tablesDirName = "tables";
    static String treeDirName = "indexes";

    public static void main(String[] args) {
        System.out.println("Welcome to the database!");
        /*String db_loc = args[0];
        int pageSize = Integer.valueOf(args[1]);
        int bufferSize = Integer.valueOf(args[2]);
        
        String index_check = args[3];*/
        String db_loc = "C:\\Users\\Tyler\\Desktop\\College Folders\\Personal Database Project\\Personal_Database_Project\\databaseLoc\\";
        int pageSize = 250;
        int bufferSize = 10;
        String index_check = "false";

        boolean index = false;
        if(index_check.equals("true") || index_check.equals("false")){
            index = Boolean.valueOf(index_check);
        }
        else{
            System.err.println("Invalid index argument");
            return;
        }

        try {

            //Check if there is a catalog file in the database directory
            //  with the name 'catalog'
            //Creates a new catalog if one doesn't exist and 
            //  saves it in database director
            //Begins loading Catalog if it does exist
            System.out.println("Looking at " + db_loc + "....");
            File catalogFile = new File(db_loc + catalogFileName);
            boolean catalogExist = false;
            if (catalogFile.exists()){
                System.out.println("Catalog exists");
                catalogExist = true;
            } else {
                System.out.println("Catalog does not exist\nCreating new Catalog");
            }
            String catalog_loc = db_loc+catalogFileName;
            Catalog catalog = new Catalog(catalog_loc, pageSize, index);

            catalog.loadCatalog();
            if (catalogFile.exists() && !catalogExist){
                System.out.println("Catalog now exists at: " + catalog_loc);
            }
            //get the page size that was loaded from catalog
            //size that was given may not be the same as the one in catalog
            //must use one in catalog
            pageSize = catalog.getPageSize();

            String tablesDirLoc = db_loc + tablesDirName;
            File tableDir = new File(tablesDirLoc);
            boolean tableDirExists = false;
            boolean dirCreated = false;
            if (tableDir.isDirectory()){
                tableDirExists = true;
                System.out.println("Table Directory Exists");
            } else {
                System.out.println("Table Directory does not exist\n"+
                                    "creating Table Directory at: "+tablesDirLoc);
                dirCreated = tableDir.mkdir();
            }

            if (dirCreated && !tableDirExists){
                System.out.println("Table Directory now exists at:\n"+tablesDirLoc);
            }

            if (catalog.isIndexOn()){
                String treesDirLoc = db_loc + treeDirName;
                File treeDir = new File(treesDirLoc);
                boolean treeDirExists = false;
                dirCreated = false;
                if (treeDir.isDirectory()){
                    treeDirExists = true;
                    System.out.println("Tree Directory Exists");
                } else {
                    System.out.println("Tree Directory does not exist\n"+
                                    "creating Tree Directory at: "+treesDirLoc);
                    dirCreated = treeDir.mkdir();
                }

                if (dirCreated && !treeDirExists){
                    System.out.println("Tree Directory now exists at:\n"+treesDirLoc);
                }
            }


            StorageManager storageManager = new StorageManager(db_loc, pageSize, bufferSize, catalog);

            System.out.print("Please enter commands, enter <quit> to shutdown the db\n");

        Scanner scanner = new Scanner(System.in);

        // gets inputs until statement ends with ';' or user has entered '<quit>'
        while(true){
            System.out.print("\nDB> ");

            String input = "";
            String raw_input;
            do{
                raw_input = scanner.nextLine();
                if(!input.equals("")){
                    input += " ";
                }
                input += raw_input;
            }while (!raw_input.endsWith(";") && !raw_input.equals("<quit>"));

            // combines multiple whitespaces into one
            input = input.replaceAll("\\s+", " ");
            
            String input_lower = input.toLowerCase();

            // quit
            if(input_lower.equals("<quit>")){
                // BD shuts down and writes everything from page buffer to hardware
                storageManager.flushBuffer();
                System.out.println("\nSafely shutting down the database...");
                System.out.println("Purging page buffer...");
                System.out.println("Saving catalog...\n");
                System.out.println("Exiting the database...");
                break;
            // insert
            }else if(input_lower.startsWith("insert")){
                SQLParser.parse_insert(catalog, input, storageManager);
            // select
            }else if(input_lower.startsWith("select")){
                SQLParser.parse_select(catalog, input, storageManager);
            // display info
            }else if(input_lower.startsWith("display info")){
                SQLParser.parse_display_info(catalog, input, storageManager);
            // display schema
            }else if(input_lower.equals("display schema;")){
                DMLParser.display_schema(catalog, db_loc, bufferSize, storageManager);
            // delete
            }else if(input_lower.startsWith("delete")){
                SQLParser.parse_delete(catalog, input, storageManager);
            // create table
            }else if(input_lower.startsWith("create table")){
                SQLParser.parseTable(input, catalog, storageManager);
            // drop table
            }else if(input_lower.startsWith("drop table")){
                SQLParser.parseTable(input, catalog, storageManager);
            // alter table
            }else if(input_lower.startsWith("alter table")){
                SQLParser.parseTable(input, catalog, storageManager);
            // update
            }else if (input_lower.startsWith("update")){
                SQLParser.parse_update(catalog, input, storageManager);
            // invalid command
            }else{
                System.out.println("Invalid Command: " + input);
            }
        }
        scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
        }
    }
}
