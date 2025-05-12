import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages the storage of pages in memory using an LRU cache.
 * Handles reading and writing pages to disk when necessary.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 * @Contributor Tyler Black, tcb863
 */
public class PageBuffer {
    private final int bufferSize; // Maximum number of pages that can be stored in memory
    private final LRUCache cache; // LRU cache for managing page memory
    private final String dbLocation; // Location of the database files
    private final int pageSize; // Size of each page in bytes

    /**
     * Constructs a PageBuffer for managing in-memory pages and file storage.
     * @param bufferSize Maximum number of pages to store in memory.
     * @param dbLocation Directory path for database storage.
     * @param pageSize The fixed size of each page.
     */
    public PageBuffer(int bufferSize, String dbLocation, int pageSize) {
        this.bufferSize = bufferSize;
        this.cache = new LRUCache(bufferSize);
        this.dbLocation = dbLocation;
        this.pageSize = pageSize;
    }

    /**
     * Stores a page in the buffer, 
     *  writing the least recently used (LRU) page to disk if necessary.
     * @param tableId the id of the table the page belongs to
     * @param pageId the unique id of the page
     * @param page the Page object ot store
     * @param pageOrder the order of the pages in the table with the given table id
     * @param shiftAmount the shift that should be accounted for when writing the page
     *                      Used when table file header is being rewritten and all pages are being shifted
     * @throws IOException
     */
    public void pushPage(int tableId, int pageId, Object page, List<Integer> pageOrder, int shiftAmount) throws IOException{
        int key;
        if (page instanceof Page){
            key = getPageKey(tableId, pageId, false);
        } else {
            key = getPageKey(tableId, pageId, true);
        }
        int excessKey = this.cache.getLRUKey();
        Object excess = this.cache.put(key, page);

        if (excess != null) {
            assert(excessKey != -1);
            int excessTableId = Math.abs(excessKey / 100000);
            int excessPageId = Math.abs(excessKey % 100000);

            String filePath;
            if (excess instanceof Page){
                filePath = dbLocation + "/tables/table" + excessTableId + ".tbl";
            } else {
                filePath = dbLocation + "/indexes/tree" + excessTableId + ".bpt";
            }
            File tableFile = new File(filePath);
            if (tableFile.exists()){
                if (tableId != excessTableId ||
                    (page instanceof Page && !(excess instanceof Page))||
                     (!(page instanceof Page) && excess instanceof Page)){
                    if (excess instanceof Page){
                        pageOrder = getPageOrder(excessTableId, true);
                    } else {
                        pageOrder = getPageOrder(excessTableId, false);
                    }
                }
                if (excess instanceof Page){
                    writePageToDisk(excessTableId, excessPageId, (Page)excess, pageOrder, shiftAmount);
                } else {
                    writeBplusNodeToDisk(excessTableId, excessPageId, (BplusTreeNode)excess, pageOrder, shiftAmount);
                }
            }  
        }

        // Ensure buffer does not exceed its limit
        if (cache.size() > bufferSize) {
            evictLRUPage(0, false);
        }
    }

    /**
     * Used for when all pages need to be shifted some amount of bytes
     *  to accomidate re-writing the tableheader
     * @param tableId the id of the table to maipulate
     * @param pageId the id of the page to shift
     * @param pageOrder the page order of the table with the given id
     * @param shiftAmount the shift that should be accounted for when writing the page
     *                      Used when table file header is being rewritten and all pages are being shifted
     * @throws IOException
     */
    public void shiftPage(int tableId, int pageId, List<Integer> pageOrder, int shiftAmount, AttributeSchema primaryKey, boolean getTreeNode) throws IOException{
        Object pageToShift = getPage(tableId, pageId, pageOrder, shiftAmount, primaryKey, getTreeNode);
        Object removed = cache.remove(getPageKey(tableId, pageId, getTreeNode));
        assert(removed != null);
        if (pageToShift instanceof Page){
            writePageToDisk(tableId, pageId, (Page)pageToShift, pageOrder, shiftAmount);
        } else {
            writeBplusNodeToDisk(tableId, pageId, (BplusTreeNode)pageToShift, pageOrder, shiftAmount);
        }
    }

    /**
     * Retrieves a page from the buffer. If not found, it is loaded from disk.
     * @param tableId The id of the table the page belongs to
     * @param pageId The unique id of the page.
     * @param pageOrder the page order of the table with the given id
     * @param shiftAmount the shift that should be accounted for when writing the page
     *                      Used when table file header is being rewritten and all pages are being shifted
     * @return the page retrieved from the buffer
     * @throws IOException
     */
    public Object getPage(int tableId, int pageId, List<Integer> pageOrder, int shiftAmount, AttributeSchema primaryKey, boolean getTreeNode) throws IOException{
        int key = getPageKey(tableId, pageId, getTreeNode);
        Object page = this.cache.get(key);

        if (page == null) {
            //System.out.println("Happening when pageId is:"+pageId);
            if (!getTreeNode){
                page = readPageFromDisk(tableId, pageId, pageOrder);
            }else {
                page = readBPlusNodeFromDisk(tableId, pageId, pageOrder, primaryKey);
            }
            pushPage(tableId, pageId, page, pageOrder, shiftAmount);
        }
        return page;
    }


    /**
     * Gets the order of pages in the table with the given tableId
     * @param tableId the id of the table to get the page order from
     * @return a list of page ids
     * @throws IOException
     */
    private List<Integer> getPageOrder(int tableId, boolean isPage) throws IOException{
        String filePath;
        if(isPage){
            filePath = dbLocation + "tables/table" + tableId + ".tbl";
        }else{
            filePath = dbLocation + "/indexes/tree" + tableId + ".bpt";
        }
        
        //String filePath = dbLocation + "tables/table" + tableId + ".tbl";
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
    }

    /**
     * Evicts the least recently used (LRU) page from memory and writes it to disk.
     * @param shiftAmount the shift that should be accounted for when writing the page
     *                      Used when table file header is being rewritten and all pages are being shifted
     * @throws IOException
     */
    private void evictLRUPage(int shiftAmount, boolean shiftTree) throws IOException{
        int keyToEvict = cache.getLRUKey();
        Object pageToEvict = cache.remove(keyToEvict);
        if (pageToEvict != null) {
            int tableId = Math.abs(keyToEvict / 100000);
            int pageId = Math.abs(keyToEvict % 100000);
            
            String filePath;
            if (pageToEvict instanceof Page){
                filePath = dbLocation + "/tables/table" + tableId + ".tbl";
            }else{
                filePath = dbLocation + "/indexes/tree" + tableId + ".bpt";
            }
            //String filePath = dbLocation + "/tables/table" + tableId + ".tbl";
            File tableFile = new File(filePath);
            if (tableFile.exists()){
                List<Integer> pageOrder;
                if (pageToEvict instanceof Page){
                    if (shiftTree){
                        shiftAmount = 0;
                    }
                    pageOrder = getPageOrder(tableId, true);
                    writePageToDisk(tableId, pageId, (Page)pageToEvict, pageOrder, shiftAmount);
                } else {
                    if (!shiftTree){
                        shiftAmount = 0;
                    }
                    pageOrder = getPageOrder(tableId, false);
                    writeBplusNodeToDisk(tableId, pageId, (BplusTreeNode)pageToEvict, pageOrder, shiftAmount);
                }
            }   
        }
    }

    /**
     * Generates a unique key for a page based on table ID and page ID.
     * @param tableId The table's unique identifier.
     * @param pageId The page's unique identifier within the table.
     * @return A computed unique integer key.
     */
    private int getPageKey(int tableId, int pageId, boolean isTree) {
        if (!isTree){
            return tableId * 100000 + pageId;
        } else {
            return tableId * -100000 - pageId;
        }
    }

    /**
     * Method for writing everything in the pageBuffer to file
     * @param shiftAmount the shift that should be accounted for when writing the page
     *                      Used when table file header is being rewritten and all pages are being shifted
     * @throws IOException
     */
    public void flush(int shiftAmount, boolean shiftTree) throws IOException{
        while(cache.size() != 0){
            evictLRUPage(shiftAmount, shiftTree);
        }
    }

    /**
     * Writes given record to a ByteBuffer for storage.
     * @param buffer the byte buffer to store record data
     * @param record the record having it's data written to buffer
     */
    public void writeRecordToBuffer(ByteBuffer buffer, Record record) {
        byte[] data = record.getData();
        buffer.putInt(data.length); // Store data length first
        buffer.put(data); // Store actual data
    }

    /**
     * Writes a page to disk in binary format.
     * @param tableId the table the page belongs to
     * @param pageId the page's unique identifier
     * @param page The Page object to store
     * @param pageOrder the page order of the table with the given id
     * @param shiftAmount the shift that should be accounted for when writing the page
     *                      Used when table file header is being rewritten and all pages are being shifted
     */
    private void writePageToDisk(int tableId, int pageId, Page page, List<Integer> pageOrder, int shiftAmount) {
        String filePath = dbLocation + "/tables/table" + tableId + ".tbl";
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
            FileChannel channel = raf.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);

            int numPages = pageOrder.size();
            //skip over the header
            raf.skipBytes(4 + 4*numPages);
            //loop through the pageIds until find the pageId passed in
            //For each pageId that isn't the one given, skip over PageSize bytes
            for (int pageNum : pageOrder){
                if (pageNum == pageId){
                    break;
                }
                int bytesSkipped = raf.skipBytes(pageSize);
                assert(bytesSkipped == pageSize);
            }


            List<Record> records = page.getRecords();
            int numRecords = records.size();
            buffer.putInt(numRecords); //put the number of records into the buffer
            //write each record into the buffer
            for (Record record : records) {
                writeRecordToBuffer(buffer, record);
            }

            buffer.position(0); //move the position of the buffer to 0
                                            //this is so writting begins from the
                                            //  beginning of the buffer
            /*if (shiftingPage){
                raf.skipBytes(4); //skip 4 bytes because int is being added to page order
            }*/
            //raf.skipBytes(shiftAmount);
            channel.position(raf.getFilePointer()+shiftAmount);
            //raf.seek(raf.getFilePointer())
            //raf.

            long bytesWritten = channel.write(buffer);
            assert (bytesWritten == pageSize);
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads a Record object from a ByteBuffer.
     * @param buffer The buffer to read from.
     * @return A Record object containing the extracted data.
     */
    private Record readRecordFromDisk(ByteBuffer buffer) {
        int length = buffer.getInt(); // Read length of data
        byte[] data = new byte[length];
        buffer.get(data); // Read actual data
        return new Record(data);
    }

    /**
     * Reads a page from disk in binary format.
     * @param tableId The table the page belongs to.
     * @param pageId The page's unique identifier.
     * @param pageOrder the page order of the table with the given id
     * @return The Page object retrieved from disk, or null if not found.
     */
    private Page readPageFromDisk(int tableId, int pageId, List<Integer> pageOrder) {
        String filePath = dbLocation + "/tables/table" + tableId + ".tbl";
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r");
                FileChannel channel = raf.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);
            
            int numPages = pageOrder.size();
            //skip over the header
            raf.skipBytes(4 + 4*numPages);
            //loop through the pageIds until find the pageId passed in
            //For each pageId that isn't the one given, skip over PageSize bytes
            for (int pageNum : pageOrder){
                if (pageNum == pageId){
                    break;
                }
                int bytesSkipped = raf.skipBytes(pageSize);
                assert(bytesSkipped == pageSize);
            }

            channel.position(raf.getFilePointer()); //move the channel to the filepointer
                                                    //this is where reading will be done from
            channel.read(buffer); //read in pageSize bytes into the buffer
            buffer.flip(); //flip buffer; buffer position 0

            Page page = new Page(pageSize, pageId);
            int numRecords = buffer.getInt(); //get the number of records
            for (int i = 0; i < numRecords; i++) {
                page.insertAtTheEnd(readRecordFromDisk(buffer)); //get the record and insert it into the page
            }
            raf.close();
            return page;
        } catch (IOException e) {
            System.out.println("HUGE PROBLEM\nLOOK HERE\nTHROWING I/O EXCEPTION");
            return null;
        }
    }

    /**
     * write out bplusnode
     */
    private void writeBplusNodeToDisk(int tableId, int pageId, BplusTreeNode node, List<Integer> pageOrder, int shiftAmount) {
        String filePath = dbLocation + "/indexes/tree" + tableId + ".bpt";
    
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
             FileChannel channel = raf.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);
    

            // Skip Header
            raf.skipBytes(4+(4*pageOrder.size()));

            // Move to correct page position
            for(int pid: pageOrder) {
                if(pid==pageId) break;
                raf.skipBytes(pageSize+12);
            }
                
            // 1. Write metadata
            buffer.putInt(node.getPageNumber());           // page number
            buffer.putInt(node.getParentPointer());        // parent pointer
            buffer.putInt(node.getValues().size());        // num entries

            //write out metadata
            buffer.flip();
            channel.position(raf.getFilePointer() + shiftAmount);
            channel.write(buffer);
            //set position back to 0 and limit back to pageSize
            buffer.clear();

    
            // 2. Write values
            for (Object value : node.getValues()) {
                AttributeType type = node.getPrimaryKeyType();
                switch (type) {
                    case BOOLEAN -> buffer.put((byte) ((Boolean) value ? 1 : 0));
                    case INTEGER -> buffer.putInt((Integer) value);
                    case DOUBLE -> buffer.putDouble((Double) value);
                    case CHAR -> {
                        String str = (String) value;
                        for (char c : str.toCharArray()) buffer.putChar(c);
                    } 
                    case VARCHAR -> {
                        String str = (String) value;
                        buffer.putInt(str.length());
                        for (char c : str.toCharArray()) buffer.putChar(c);
                    }
                }
            }
    
            // 3. Write pointers (numEntries + 1)
            for (int[] pointer : node.getPointers()) {
                buffer.putInt(pointer[0]);  // page number
                buffer.putInt(pointer[1]);  // index
            }
    
            // 4. Write to file
            buffer.position(0);
            channel.position(raf.getFilePointer());
            int amountWritten = channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * read in bplusnode
     */
    private BplusTreeNode readBPlusNodeFromDisk(int tableId, int pageId, List<Integer> pageOrder, AttributeSchema primaryKey) {
        String filePath = dbLocation + "/indexes/tree" + tableId + ".bpt";
    
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r");
             FileChannel channel = raf.getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);
    
            // Skip Header
            raf.skipBytes(4+(4*pageOrder.size()));


            // Move to correct page position
            for(int pid: pageOrder) {
                if(pid==pageId) break;
                raf.skipBytes(pageSize+12);
            }

            channel.position(raf.getFilePointer());
            int numBytesRead = channel.read(buffer);
            buffer.flip();
    
            // 1. Metadata
            int actualPageNum = buffer.getInt();
            int parentPointer = buffer.getInt();
            int numEntries = buffer.getInt();

            //clear buffer; set position pass the metadata
            buffer.clear();
            channel.position(raf.getFilePointer() - numBytesRead + 12);
            channel.read(buffer);
            buffer.flip();
    
            // 2. Read values
            List<Object> values = new ArrayList<>();
            AttributeType keyType = primaryKey.getType();
            for (int i = 0; i < numEntries; i++) {
                switch (keyType) {
                    case BOOLEAN -> values.add(buffer.get() == 1);
                    case INTEGER -> values.add(buffer.getInt());
                    case DOUBLE -> values.add(buffer.getDouble());
                    case CHAR ->{
                        StringBuilder sb = new StringBuilder();
                        for (int j = 0; j < primaryKey.getSize(); j++) sb.append(buffer.getChar());
                        values.add(sb.toString());
                    } 
                    case VARCHAR -> {
                        /*TODO: Char doesn't have an int; needs to be it's own thing */
                        int len = buffer.getInt();
                        StringBuilder sb = new StringBuilder();
                        for (int j = 0; j < len; j++) sb.append(buffer.getChar());
                        values.add(sb.toString());
                    }
                }
            }
    
            // 3. Read pointers
            List<int[]> pointers = new ArrayList<>();
            for (int i = 0; i < numEntries + 1; i++) {
                int page = buffer.getInt();
                int index = buffer.getInt();
                pointers.add(new int[]{page, index});
            }
    

            // 4.5 calculate n for the BplusTreeNode
            int keyPointerSize;
            if (keyType == AttributeType.CHAR || keyType == AttributeType.VARCHAR){
                keyPointerSize = primaryKey.getSize()*2 + 8;
            } else{
                keyPointerSize = primaryKey.getSize() + 8;
            }
            int n = (int)Math.floor(((double)pageSize)/keyPointerSize) - 1;
            
    
            // 5. Reconstruct node
            BplusTreeNode node = new BplusTreeNode(n, actualPageNum, tableId, parentPointer, primaryKey.getType());
            node.setValues(values);
            node.setPointers(pointers);
    
            return node;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    
}
