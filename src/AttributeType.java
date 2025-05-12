/**
 * Enum defining possible attribute types for database table columns.
 * These types define the structure of the stored data.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 */
public enum AttributeType {
    INTEGER,  // Whole numbers
    DOUBLE,   // Floating point numbers
    BOOLEAN,  // True/false values
    CHAR,     // Fixed-length character strings
    VARCHAR,  // Variable-length character strings
    NULL      // Null values
}
