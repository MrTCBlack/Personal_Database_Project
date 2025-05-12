import java.util.regex.*;

/**
 * Tokenizes user input for the DDLParser.
 * 
 * @author Brayden Mossey, bjm9599@rit.edu 
 */
public class DDLTokenizer {

    private static final Pattern TOKEN_PATTERN = Pattern.compile(

        "(?i)(CREATE|TABLE|DROP|ALTER|ADD|PRIMARYKEY|NOTNULL|UNIQUE|DEFAULT|DOUBLE|INTEGER|BOOLEAN|CHAR|VARCHAR)|" +
        "([A-Za-z_][A-Za-z0-9_]*)|" + 
        "(\\d+\\.?\\d*)|" +          
        "('.*?')|" +                 
        "(,|;|\\(|\\))"              

    );

    private final Matcher matcher;
    private String currentToken;
    private TokenType currentTokenType;

    public DDLTokenizer(String input){

        this.matcher = TOKEN_PATTERN.matcher(input);
        advance();

    }

    /**
     * Advance to the next token
     */
    public void advance(){

        if(this.matcher.find()){

            if(this.matcher.group(1) != null){

                this.currentToken = matcher.group(1).toUpperCase();
                this.currentTokenType = TokenType.valueOf(this.currentToken);

            }else if(this.matcher.group(2) != null){

                this.currentToken = matcher.group(2);
                this.currentTokenType = TokenType.IDENTIFIER;

            }else if(this.matcher.group(3) != null){

                this.currentToken = matcher.group(3);
                this.currentTokenType = TokenType.NUMBER;

            }else if(this.matcher.group(4) != null){

                this.currentToken = matcher.group(4);
                this.currentTokenType = TokenType.STRING;

            }else{

                this.currentToken = matcher.group(5);
                this.currentTokenType = switch(this.currentToken){

                    case "," -> TokenType.COMMA;
                    case ";" -> TokenType.SEMICOLON;
                    case "(" -> TokenType.LPAREN;
                    case ")" -> TokenType.RPAREN;
                    default -> TokenType.UNKNOWN;

                };

            }

        }else{

            this.currentToken = null;
            this.currentTokenType = TokenType.EOF;

        }

    }

    /**
     * get the current token type
     * @return the current token type
     */
    public TokenType getTokenType(){

        return this.currentTokenType;

    }

    /**
     * get the current token
     * @return the current token
     */
    public String getToken(){

        return this.currentToken;

    }

}
