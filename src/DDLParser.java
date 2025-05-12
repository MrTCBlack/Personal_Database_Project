import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Parses DDL statements (CREATE, DROP, ALTER) and updates the catalog accordingly.
 * Error messages from the sample run match, any other errors that might occur may not.
 * 
 * @author Brayden Mossey, bjm9599@rit.edu
 * @contributor Justin Talbot, jmt8032@rit.edu 
 */
public class DDLParser {
    private final DDLTokenizer tokenizer;
    private final Catalog catalog;
    private final Set<TokenType> varTypes;
    private final Map<TokenType, Integer> varSizes;
    private final StorageManager storageManager;

    /**
     * DDL constructor
     * @param input the user given input to be parsed
     * @param catalog the catalog of the database
     */
    public DDLParser(String input, Catalog catalog, StorageManager storageManager) {
        this.tokenizer = new DDLTokenizer(input);
        this.catalog = catalog;
        this.varTypes = new HashSet<>(Arrays.asList(
            TokenType.DOUBLE,
            TokenType.INTEGER,
            TokenType.CHAR,
            TokenType.BOOLEAN,
            TokenType.VARCHAR
        ));
        this.varSizes = new HashMap<>(){{
            put(TokenType.INTEGER, 4);
            put(TokenType.DOUBLE, 8);
            put(TokenType.BOOLEAN, 1);
        }};
        this.storageManager = storageManager;
    }

    /**
     * Parses the DDL commands.
     */
    public void parse() throws Exception {
        while (tokenizer.getTokenType() != TokenType.EOF) {
            switch (tokenizer.getTokenType()) {
                case CREATE -> parseCreateTable();
                case DROP -> parseDropTable();
                case ALTER -> parseAlterTable();
                default -> {
                    tokenizer.advance();
                }
            }
        }
    }

    /**
     * Parses CREATE TABLE statements.
     * Follows the format specified in the project writeup.
     */
    private void parseCreateTable() throws Exception {
        tokenizer.advance();
        if (!expect(TokenType.TABLE, "Expected TABLE after CREATE")) return;

        String tableName = tokenizer.getToken();
        tokenizer.advance();

        if (!expect(TokenType.LPAREN, "Expected '(' after table name")) return;

        List<AttributeSchema> attributes = new ArrayList<>();
        boolean hasPrimaryKey = false;

        while (tokenizer.getTokenType() != TokenType.RPAREN && tokenizer.getTokenType() != TokenType.EOF) {
            int size = 0;
            String colName = tokenizer.getToken();
            tokenizer.advance();

            if (!varTypes.contains(tokenizer.getTokenType())) {
                System.err.println("Invalid data type \"" + tokenizer.getToken() + "\"");
                System.out.println("ERROR");
                return;
            }

            String colType = tokenizer.getToken();
            tokenizer.advance();

            if (tokenizer.getTokenType() == TokenType.LPAREN) {
                tokenizer.advance();
                try {
                    size = Integer.parseInt(tokenizer.getToken());
                } catch (NumberFormatException e) {
                    System.err.println("Invalid size for CHAR/VARCHAR.");
                    return;
                }
                tokenizer.advance();

                if (!expect(TokenType.RPAREN, "Expected ')' after size")) return;
            }

            List<TokenType> constraints = new ArrayList<>();
            while (tokenizer.getTokenType() == TokenType.PRIMARYKEY || tokenizer.getTokenType() == TokenType.NOTNULL ||
                   tokenizer.getTokenType() == TokenType.UNIQUE) {
                constraints.add(tokenizer.getTokenType());
                tokenizer.advance();
            }

            if (constraints.contains(TokenType.PRIMARYKEY)) {
                if (hasPrimaryKey) {
                    System.err.println("More than one primarykey");
                    System.out.println("ERROR");
                    return;
                }
                hasPrimaryKey = true;
            }

            if(TokenType.valueOf(colType) == TokenType.CHAR || TokenType.valueOf(colType) == TokenType.VARCHAR){
                attributes.add(new AttributeSchema(
                    colName, AttributeType.valueOf(colType), size,
                    constraints.contains(TokenType.PRIMARYKEY),
                    constraints.contains(TokenType.UNIQUE),
                    constraints.contains(TokenType.NOTNULL)
                ));
            }else{
                attributes.add(new AttributeSchema(
                    colName, AttributeType.valueOf(colType), varSizes.get(TokenType.valueOf(colType)),
                    constraints.contains(TokenType.PRIMARYKEY),
                    constraints.contains(TokenType.UNIQUE),
                    constraints.contains(TokenType.NOTNULL)
                ));
            }

            if (tokenizer.getTokenType() == TokenType.COMMA) {
                tokenizer.advance();
            }
        }

        if (!expect(TokenType.RPAREN, "Expected ')' at the end of column definitions")) return;
        if (!expect(TokenType.SEMICOLON, "Expected ';' at the end of statement")) return;

        if(attributes.isEmpty()){
            System.err.println("Table with no attributes");
            System.out.println("ERROR");
            return;
        }

        if(hasPrimaryKey){
            createTable(tableName, attributes);
        }else{
            System.out.println("No primary key defined");
            System.out.println("ERROR");
        }
    }

    /**
     * Parses DROP TABLE statements.
     */
    private void parseDropTable() throws Exception {
        tokenizer.advance();
        if (!expect(TokenType.TABLE, "Expected TABLE after DROP")) return;

        String tableName = tokenizer.getToken();
        tokenizer.advance();

        if (!expect(TokenType.SEMICOLON, "Expected ';' at the end of statement")) return;

        dropTable(tableName);
    }

    /**
     * Parses ALTER TABLE statements.
     */
    private void parseAlterTable() throws Exception{
        tokenizer.advance();
        if (!expect(TokenType.TABLE, "Expected TABLE after ALTER")) return;

        String tableName = tokenizer.getToken();
        tokenizer.advance();

        if (tokenizer.getTokenType() == TokenType.ADD) {
            tokenizer.advance();
            String colName = tokenizer.getToken();
            tokenizer.advance();

            if (!varTypes.contains(tokenizer.getTokenType())) {
                System.err.println("Expected valid column type after column name.");
                return;
            }

            String colType = tokenizer.getToken();
            tokenizer.advance();
            int size = 0;
            String defaultValue = null;

            if (tokenizer.getTokenType() == TokenType.LPAREN) {
                tokenizer.advance();
                try {
                    size = Integer.parseInt(tokenizer.getToken());
                } catch (NumberFormatException e) {
                    System.err.println("Invalid size for CHAR/VARCHAR.");
                    return;
                }
                tokenizer.advance();

                if (!expect(TokenType.RPAREN, "Expected ')' after size")) return;
            }else{
                size = varSizes.get(TokenType.valueOf(colType));
            }

            if (tokenizer.getTokenType() == TokenType.DEFAULT) {
                tokenizer.advance();
                defaultValue = tokenizer.getToken();
                tokenizer.advance();
            }

            if (!expect(TokenType.SEMICOLON, "Expected ';' at the end of statement")) return;

            try {
                alterTable(tableName, new AttributeSchema(colName, AttributeType.valueOf(colType), size, false, false, false), defaultValue);
            } catch (Exception e) {
                System.out.println("ERROR");
                e.printStackTrace();
            }
        } else if (tokenizer.getTokenType() == TokenType.DROP) {
            tokenizer.advance();
            String colName = tokenizer.getToken();
            tokenizer.advance();

            if (!expect(TokenType.SEMICOLON, "Expected ';' at the end of statement")) return;

            alterTable(tableName, colName);
        } else {
            System.err.println("Syntax Error: Unexpected token in ALTER TABLE");
        }
    }

    /**
     * Creates a table and adds it to the catalog.
     */
    private void createTable(String tableName, List<AttributeSchema> attributes) throws Exception {
        TableSchema tableSchema = new TableSchema(tableName, catalog.getLastUsed() + 1);
        Set<String> columnNames = new HashSet<>();

        if(catalog.getTableSchemaNameMap().keySet().contains(tableName)){
            System.out.println("Table of name " + tableName + " already exists");
            System.out.println("ERROR");
            return;
        }

        for (AttributeSchema attr : attributes) {
            if (!columnNames.add(attr.getName())) {
                System.err.println("Duplicate attribute name \"" + attr.getName() + "\"");
                System.out.println("ERROR");
                return;
            }
            tableSchema.addAttribute(attr);
        }

        catalog.addTable(tableSchema, catalog.isIndexOn());
        System.out.println("SUCCESS");
    }

    /**
     * Drops a table from the catalog.
     */
    private void dropTable(String tableName) throws Exception{

        if(!catalog.getTableSchemaNameMap().keySet().contains(tableName)){
            System.err.println("Table with name \"" + tableName + "\" does not exist");
            System.out.println("ERROR");
            return;
        }

        this.catalog.removeTableByName(tableName);
        this.catalog.saveCatalog();
        System.out.println("SUCCESS");
    }

    /**
     * Alters a table schema by adding an attribute.
     */
    private void alterTable(String tableName, AttributeSchema attr, String defaultValue) throws Exception {
        TableSchema tableSchema = catalog.getTableSchemaByName(tableName);
        if (tableSchema == null) {
            System.err.println("Table '" + tableName + "' not found.");
            System.out.println("ERROR");
            return;
        }
        for(AttributeSchema attrr : tableSchema.getAttributes()){
            if(attrr.getName().equals(attr.getName())){
                System.err.println("Attribute '" + attr.getName() + "' already exists");
                System.out.println("ERROR");
                return;
            }
        }

        //creates a new TableSchema object that is exactly the same except
        //  for a new attribute has been added
        TableSchema updatedTableSchema = this.catalog.addColumn(attr, tableName);
        //adds the given attribute to all records with the given default values
        storageManager.addAttribute(attr, defaultValue, tableSchema.getTableNum());
        //updates the catalog so that old TableSchema is removed and new TableSchema is added
        this.catalog.updateTableSchema(tableSchema, updatedTableSchema);
        //saves the catalog
        this.catalog.saveCatalog();

        System.out.println("SUCCESS");
    }

    /**
     * Alters a table schema by dropping an attribute.
     */
    private void alterTable(String tableName, String colName) throws Exception{
        TableSchema tableSchema = catalog.getTableSchemaByName(tableName);
        if (tableSchema == null) {
            System.err.println("Table '" + tableName + "' not found.");
            System.out.println("ERROR");
            return;
        }
        if(tableSchema.getAttributes().stream().filter(attr -> attr.getName().equals(colName)).collect(Collectors.toSet()).isEmpty()){
            System.err.println("Attribute '" + colName + "' does not exist");
            System.out.println("ERROR");
            return;
        }
        if(tableSchema.getAttributes().stream().filter(attr -> attr.isPrimaryKey()).collect(Collectors.toList()).get(0).getName().equals(colName)){
            System.err.println("Cannot remove primary key");
            System.out.println("ERROR");
            return;
        }

        //creates a new TableSchema object that is exactly the same except
        //  for the column with the given column has been removed
        TableSchema updatedTableSchema = this.catalog.removeColumn(colName, tableName);
        //removes the attribute with the given name from all records
        storageManager.removeAttribute(colName, tableSchema.getTableNum());
        //updates the catalog so that old TableSchema is removed and new TableSchema is added
        this.catalog.updateTableSchema(tableSchema, updatedTableSchema);
        //saves the catalog
        this.catalog.saveCatalog();
        System.out.println("SUCCESS");
    }

    /**
     * Helper method to check for expected tokens.
     */
    private boolean expect(TokenType expected, String errorMessage) {
        if (tokenizer.getTokenType() != expected) {
            System.err.println("Syntax Error: " + errorMessage);
            return false;
        }
        tokenizer.advance();
        return true;
    }
}
