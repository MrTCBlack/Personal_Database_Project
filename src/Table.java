import java.util.ArrayList;
import java.util.List;

/**
 * Represents a database table that consists of multiple pages.
 * This class manages the storage and retrieval of records within the table.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 * @Contributor Tyler Black tcb8683
 */
public class Table {
    private final String tableName; // The name of the table
    private List<Page> pages; // List of pages storing records
    private final int pageSize; // Fixed size of each page in bytes
    private List<Integer> pageOrder; // Order of the PageId's

    /**
     * Constructs a Table with the given name and page size.
     * @param tableName The name of the table.
     * @param pageSize The fixed size of each page in bytes.
     */
    public Table(String tableName, int pageSize, ArrayList<Integer> pageOrder) {
        this.tableName = tableName;
        this.pageSize = pageSize;
        this.pages = new ArrayList<>();
        this.pageOrder = pageOrder;
    }

    /**
     * Gets the name of the table.
     * @return The table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets the page order of this table
     * @return a list of page ids
     */
    public List<Integer> getPageOrder(){
        return pageOrder;
    }

    /**
     * Gets the pages of this table
     * @return a list of Page objects
     */
    public List<Page> getPages(){
        return pages;
    }

    /**
     * inserts a page id into a page order depending on given index
     * @param pageId the id of the page
     * @param pageOrderIndex index of the page order
     */
    public void insertPageIdIntoOrder(int pageId, int pageOrderIndex){
        List<Integer> newPageOrder = new ArrayList<>();
        int count = 0;
        boolean breakOut = false;
        while (count < pageOrder.size()){
            newPageOrder.add(pageOrder.get(count));
            if (count == pageOrderIndex){
                newPageOrder.add(pageId);
                count += 1;
                breakOut = true;
                break;
            }
            count += 1;
        }
        while(count < pageOrder.size()){
            newPageOrder.add(pageOrder.get(count));
        }
        if (!breakOut){
            newPageOrder.add(pageId);
        }
        pageOrder = newPageOrder;
    }

    /**
     * adds a given page to the table
     * @param page page to be added
     */
    public void addPage(Page page, int indexToEnterIn, int pageId) {
        List<Page> newPages = new ArrayList<>();
        int count = 0;
        while (count < indexToEnterIn){
            newPages.add(pages.get(count));
            count += 1;
        }
        newPages.add(page);
        while (count < pages.size()){
            newPages.add(pages.get(count));
            count += 1;
        }
        pages = newPages;
        insertPageIdIntoOrder(pageId, indexToEnterIn);
        //pages.add(page);
    }

    /**
     * Inserts a page into this tables Page list
     * @param page the page to be inserted
     */
    public void insertPage(Page page){
        pages.add(page);
    }
}
