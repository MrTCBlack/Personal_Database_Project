import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.io.*;


/**
 * Manages all logic for and represents a B+ tree
 * 
 * @author Tyler Black tcb8683, Teagan Harvey tph6529
 */
public class BplusTreeNode {
    private List<Object> values;
    private List<int[]> pointers;
    private int parentPointer;
    private int pageNumber;
    private AttributeType primaryKeyType;
    private int n;
    private int treeId;
    private static BplusTreeNode rootNode;

    public BplusTreeNode(int n, int pageNumber, int treeId, int parentPointer, AttributeType primaryKeyType){
        this.values = new ArrayList<Object>();
        this.pointers = new ArrayList<int[]>();
        this.parentPointer = parentPointer;
        this.primaryKeyType = primaryKeyType;
        this.treeId = treeId;
        this.pageNumber = pageNumber;
        this.n = n;
    }

    /**
     * return the set of values in this node
     */
    public List<Object> getValues(){
        return values;
    }

    /**
     * return the set of pointers in this node
     */
    public List<int[]> getPointers(){
        return pointers;
    }

    /**
     * get the page number of this node
     */
    public int getPageNumber() { 
        return pageNumber; 
    }

    /**
     * get the parent of this node
     */
    public int getParentPointer() {
        return parentPointer; 
    }
    
    /**
     * get the primary key type of every value in this node
     */
    public AttributeType getPrimaryKeyType() {
        return primaryKeyType; 
    }

    // These 3 setters are used when reading/writing b+ tree nodes
    /**
     * set the values list for this node
     */
    public void setValues(List<Object> values) {
        this.values = values;
    }

    /**
     * set the pointers list for this node
     */
    public void setPointers(List<int[]> pointers) {
        this.pointers = pointers;
    }

    /**
     * function for seeing if the newValue Object is lessThan the value Object
     */
    private boolean lessThan(Object newValue, Object value){
        if (primaryKeyType == AttributeType.BOOLEAN){
            if (((Boolean)newValue).equals(false) && ((Boolean)value).equals(true)){
                return true;
            }
            return false;
        } else if (primaryKeyType == AttributeType.INTEGER){
            return (Integer)newValue < (Integer)value;
        } else if (primaryKeyType == AttributeType.DOUBLE){
            return (Double)newValue < (Double)value;
        } else {
            String newValueString = (String)newValue;
            String valueString = (String)value;
            int result = newValueString.compareTo(valueString);
            if (result < 0){
                return true;
            }
            return false;
        }
    }

    /**
     * adds a new value to the B+ tree by traversing to leaf layer, if full, splits and passes value up
     * @param newValue value to add
     * @return pointer to insert value in table
     * @throws IOException
     */
    public int[] addNewValue(Object newValue) throws IOException{
        // if empty node, set default values
        if (values.size() == 0){
            values.add(newValue);
            int[] pointer = {1, 0};
            pointers.add(pointer);
            int[] lastPointer = {-1, -2};
            pointers.add(lastPointer);
            //push this node to the buffer
            //List<Integer> pageOrder = StorageManager.getPageOrder(this.treeId, true);
            StorageManager.pushBplusNode(this.treeId, this.pageNumber, this);
            return pointer;
        }
        int[] pointer = null;
        int value_index = 0;
        // loop through list; if less than value, go to pointer on the left
        for (value_index = 0; value_index < values.size(); value_index++){
            if(newValue.equals(values.get(value_index))){
                pointer = pointers.get(value_index);
                if(pointer[1] != -1){
                    return null;
                }
            }else if (lessThan(newValue, values.get(value_index))){
                pointer = pointers.get(value_index);
                break;
            }
        }
        // else go to last pointer
        if(pointer == null){
            //use values.size() because pointers is always 1 greater than values
            // so last index of pointers would be values.size()
            pointer = pointers.get(values.size());
        }

        // is leaf node
        if(pointer[1] != -1){
            // add value to node
            values.add(value_index, newValue);
            //add new pointer to node
            int[] value_pointer = new int[2];
            if(value_index > 0){
                value_pointer[0] = pointers.get(value_index - 1)[0];
                value_pointer[1] = pointers.get(value_index - 1)[1]+1;
            }else{
                value_pointer[0] = pointers.get(value_index)[0];
                value_pointer[1] = pointers.get(value_index)[1];
            }
            pointers.add(value_index, value_pointer);


            BplusTreeNode currentNode = this;
            int currentIndex = value_index + 1;
            int tablePageNumber = this.pointers.get(value_index)[0];
            List<int[]> currentPointers = this.pointers;
            //List<Integer> pageOrder = StorageManager.getPageOrder(treeId, true);

            // if last pointer in node, get next node
            boolean end = false;
            if(currentIndex == pointers.size() - 1){
                if (currentPointers.get(currentIndex)[0] == -1){
                    end = true;
                }else{
                    currentNode = StorageManager.getBplusNode(treeId, currentPointers.get(currentIndex)[0]);
                    currentIndex = 0;
                    currentPointers = currentNode.pointers;
                }
            }
            // loop through pointers after inserted pointer and add 1 to index until the pointer is to new tablePage
            //This will always push the updates to this node, so don't have to worry about that later
            while(!end){
                if (currentPointers.get(currentIndex)[0] != tablePageNumber){
                    break;
                }

                int[] updated_pointer = currentPointers.get(currentIndex);
                updated_pointer[1] += 1;
                currentNode.pointers.set(currentIndex, updated_pointer);

                // if last value pointer in node, get next node
                if (currentIndex == currentPointers.size() - 2){
                    //push current node before getting new
                    StorageManager.pushBplusNode(this.treeId,currentNode.pageNumber, currentNode);
                    //Same problem as up above
                    if (currentPointers.get(currentPointers.size() -1)[0] == -1){
                        break;
                    }
                    currentNode = StorageManager.getBplusNode(treeId, currentPointers.get(currentPointers.size() -1)[0]);
                    currentIndex = 0;
                    currentPointers = currentNode.pointers;
                } else {
                    currentIndex += 1;
                }
            }

            // can't insert
            if(values.size() > n){
                // calculate where to split
                int split_index = (int)Math.ceil(n / 2.0);
                Object split_value = values.get(split_index);

                List<Object> oldValues = new ArrayList<>(values);
                List<int[]> oldPointers = new ArrayList<>(pointers);
                
                // split node in half
                //BplusTreeNode rightNode = new BplusTreeNode(n, Collections.max(pageOrder)+1, this.treeId ,parentPointer, primaryKeyType);
                //LOGIC: Because always adding new page at the end, new pageID is the number of pages plus 1
                BplusTreeNode rightNode = new BplusTreeNode(n, Catalog.getTreeNumPages(this.treeId)+1, this.treeId ,parentPointer, primaryKeyType);

                // copy values and pointers from old node to split nodes
                List<Object> leftNodeValuesSublist = oldValues.subList(0, split_index);
                this.values = new ArrayList<>(leftNodeValuesSublist);
                
                List<int[]> leftNodePointersSublist = oldPointers.subList(0, split_index);
                this.pointers = new ArrayList<>(leftNodePointersSublist);

                List<Object> rightNodeValuesSublist = oldValues.subList(split_index, oldValues.size());
                rightNode.values = new ArrayList<>(rightNodeValuesSublist);

                List<int[]> rightNodePointersSublist = oldPointers.subList(split_index, oldPointers.size());
                rightNode.pointers = new ArrayList<>(rightNodePointersSublist);

                //Need to add a pointer in left pointers that points to right page
                int[] newLeftNodePointer = {rightNode.pageNumber, -2};
                this.pointers.add(newLeftNodePointer);

                //put the new page at the end
                //pageOrder = StorageManager.rewriteTableFileHeader(treeId, pageOrder.size(), Collections.max(pageOrder)+1, true);
                Catalog.addTreesNumPages(this.treeId);

                //push leftNode
                StorageManager.pushBplusNode(treeId, pageNumber, this);
                //push rightNode
                StorageManager.pushBplusNode(treeId, rightNode.pageNumber, rightNode);

                //handle what happens at root
                if (this.parentPointer == -1){
                    //BplusTreeNode newParent = new BplusTreeNode(n,Collections.max(pageOrder)+1, this.treeId,-1, primaryKeyType);
                    BplusTreeNode newParent = new BplusTreeNode(n,Catalog.getTreeNumPages(this.treeId)+1, this.treeId,-1, primaryKeyType);
                    //pageOrder = StorageManager.rewriteTableFileHeader(this.treeId, pageOrder.size(), Collections.max(pageOrder)+1, true);
                    Catalog.addTreesNumPages(this.treeId);
                    StorageManager.pushBplusNode(this.treeId, newParent.pageNumber, newParent);

                    this.setParent(newParent.pageNumber);
                    StorageManager.pushBplusNode(this.treeId, this.pageNumber, this);
                    rightNode.setParent(newParent.pageNumber);
                    StorageManager.pushBplusNode(this.treeId, rightNode.pageNumber, rightNode);
                    newParent.reflectUp(rightNode.values.get(0), this, rightNode);
                    setRoot(newParent);
                }else {
                    BplusTreeNode parent = StorageManager.getBplusNode(this.treeId, this.parentPointer);
                    parent.reflectUp(rightNode.values.get(0), this, rightNode);
                }
            }
            return value_pointer;
        // not a leaf node
        } else {
            int childPageNum = pointer[0];
            //BplusTreeNode childNode = pages.get(childPageNum);
            //List<Integer> pageOrder = StorageManager.getPageOrder(this.treeId, true);
            BplusTreeNode childNode = StorageManager.getBplusNode(this.treeId, childPageNum);
            return childNode.addNewValue(newValue);
        }
    }

    /**
     * adds a value to the given node and if full, splits, updates children pointers, and passes value up
     * @param value value to be added
     * @param leftNode left node child
     * @param rightNode right node child
     * @throws IOException
     */
    private void reflectUp(Object value, BplusTreeNode leftNode, BplusTreeNode rightNode) throws IOException{
        if (values.size() == 0){
            values.add(value);
            int[] pointer = {leftNode.pageNumber, -1};
            pointers.add(pointer);
            int[] lastPointer = {rightNode.pageNumber, -1};
            pointers.add(lastPointer);
            //push this node to the buffer
            //List<Integer> pageOrder = StorageManager.getPageOrder(this.treeId, true);
            StorageManager.pushBplusNode(this.treeId, this.pageNumber, this);
            return;
        }
        int value_index = 0;
        // loop through list;
        for (value_index = 0; value_index < values.size(); value_index++){
            if (lessThan(value, values.get(value_index))){
                break;
            }
        }

        // add value to node
        values.add(value_index, value);
        //temp pointer
        int[] temp_value_pointer = {-1, -1};
        pointers.add(value_index, temp_value_pointer);

        int[] left_node_pointer = {leftNode.pageNumber, -1};
        pointers.set(value_index, left_node_pointer);

        int[] right_node_pointer = {rightNode.pageNumber, -1};
        pointers.set(value_index+1, right_node_pointer);

        //push this node so updates are saved
        //List<Integer> pageOrder = StorageManager.getPageOrder(this.treeId, true);

        // can't insert; needs to split
        if (values.size() > n){

            // calculate where to split
            int split_index = (int)Math.ceil(n / 2.0);
            Object split_value = values.get(split_index);
            
            // split node in half
            BplusTreeNode newRightNode = new BplusTreeNode(n, Catalog.getTreeNumPages(this.treeId)+1, this.treeId, parentPointer, primaryKeyType);
            
            // copy values and pointers from old node to split nodes
            List<Object> oldValues = new ArrayList<>(values);
            List<int[]> oldPointers = new ArrayList<>(pointers);

            List<Object> newLeftNodeValuesSublist = oldValues.subList(0, split_index);
            this.values = new ArrayList<>(newLeftNodeValuesSublist);
            //for interior, need to get one more pointer than split_index
            List<int[]> newLeftNodePointersSublist = oldPointers.subList(0, split_index+1);
            this.pointers = new ArrayList<>(newLeftNodePointersSublist);

            List<Object> newRightNodeValuesSublist = oldValues.subList(split_index, oldValues.size());
            newRightNode.values = new ArrayList<>(newRightNodeValuesSublist);
            List<int[]> newRightNodePointersSublist = oldPointers.subList(split_index+1, oldPointers.size());
            newRightNode.pointers = new ArrayList<>(newRightNodePointersSublist);

            //Need to remove first in the right because it is reflecting up 
            Object reflectUpValue = newRightNode.values.remove(0);

            //put the new page at the end
            //pageOrder = StorageManager.rewriteTableFileHeader(treeId, pageOrder.size(), Collections.max(pageOrder)+1, true);
            Catalog.addTreesNumPages(treeId);

            //push leftNode
            StorageManager.pushBplusNode(treeId, this.pageNumber, this);
            //push rightNode
            StorageManager.pushBplusNode(treeId, newRightNode.pageNumber, newRightNode);

            // update all children of right node to point to right node as parent
            for(int[] pointer : newRightNode.pointers) {
                if(pointer[0] != -1){
                    BplusTreeNode child = StorageManager.getBplusNode(treeId, pointer[0]);
                    child.setParent(newRightNode.pageNumber);
                    StorageManager.pushBplusNode(treeId, child.pageNumber, child);
                } 
            }

            //handles what happens at a root
            if (this.parentPointer == -1){
                BplusTreeNode newParent = new BplusTreeNode(n,Catalog.getTreeNumPages(treeId)+1, this.treeId, -1, primaryKeyType);
                //pageOrder = StorageManager.rewriteTableFileHeader(this.treeId, pageOrder.size(), Collections.max(pageOrder)+1, true);
                Catalog.addTreesNumPages(treeId);
                StorageManager.pushBplusNode(this.treeId, newParent.pageNumber, newParent);

                this.setParent(newParent.pageNumber);
                StorageManager.pushBplusNode(this.treeId, this.pageNumber, this);
                newRightNode.setParent(newParent.pageNumber);
                StorageManager.pushBplusNode(this.treeId, newRightNode.pageNumber, newRightNode);
                newParent.reflectUp(reflectUpValue, this, newRightNode);
                setRoot(newParent);
            } else {
                BplusTreeNode parent = StorageManager.getBplusNode(this.treeId, this.parentPointer);
                parent.reflectUp(reflectUpValue, this, newRightNode);
            }
        }
    }

    /**
     * Sets the root of the tree
     * @param newRoot the root to be set
     * @throws IOException
     */
    public static void setRoot(BplusTreeNode newRoot) throws IOException{
        rootNode = newRoot;
        Catalog.setRoot(newRoot.treeId, newRoot.pageNumber);
    }

    /**
     * sets the parent pointer of current node
     * @param parentPointer node to be set as parent
     */
    private void setParent(int parentPointer){
        this.parentPointer = parentPointer;
    }

    /**
     * traverses the tree to leaf layer based on given valuer and returns leaf
     * @param value value to traverse by
     * @param pageOrder order of nodes
     * @return leaf with value
     * @throws IOException
     */
    private BplusTreeNode traversBplusTree(Object value) throws IOException{
        //if it made it to the leaf node, return the leaf node
        if (this.pointers.get(0)[1] != -1){
            return this;
        }

        for (int i = 0; i < this.values.size(); i++){
            if (lessThan(value, values.get(i))){
                BplusTreeNode child = StorageManager.getBplusNode(this.treeId, this.pointers.get(i)[0]);
                return child.traversBplusTree(value);
            }
        }
        BplusTreeNode child = StorageManager.getBplusNode(this.treeId, this.pointers.get(values.size())[0]);
        return child.traversBplusTree(value);
    }

    /**
     * Updates a range of pointers between two given index keys
     * @param first first index key in rnage
     * @param last last index key in range
     * @param newPageId new page id to set
     * @throws IOException
     */
    public void updatePagePointer(Object first, Object last, int newPageId) throws IOException{
        //List<Integer> pageOrder = StorageManager.getPageOrder(this.treeId, true);
        BplusTreeNode currentNode = traversBplusTree(first);
        int currentIndex = 0; //index in the pointers array
        while (lessThan(currentNode.values.get(currentIndex), first)){
            currentIndex += 1;
        }
        List<int[]> currentPointers = currentNode.pointers;
        Object currentValue = first;
        int pageIndex = 0; //index in the table page

        // if last pointer in node, get next node
        boolean end = false;
        if(currentIndex == currentPointers.size() - 1){
            if (currentPointers.get(currentIndex)[0] == -1){
                end = true;
            }else{
                currentNode = StorageManager.getBplusNode(this.treeId, currentPointers.get(currentIndex)[0]);
                currentIndex = 0;
                currentPointers = currentNode.pointers;
            }
        }
        // loop through pointers after inserted pointer and add 1 to index until the pointer is to new tablePage
        while(!end){
            // updates pagePointer and index in BplusNodes to match new tablePage
            int[] updated_pointer = currentPointers.get(currentIndex);
            updated_pointer[0] = newPageId;
            updated_pointer[1] = pageIndex;
            currentNode.pointers.set(currentIndex, updated_pointer);
            
            if (!lessThan(currentValue, last)){
                //push current node
                StorageManager.pushBplusNode(this.treeId,currentNode.pageNumber, currentNode);
                break;
            }

            pageIndex += 1;

            // if last value pointer in node, get next node
            if (currentIndex == currentPointers.size() - 2){
                //push current node before getting new
                StorageManager.pushBplusNode(this.treeId,currentNode.pageNumber, currentNode);
                //currentNode = pages.get(currentPointers.get(currentPointers.size() -1)[0]);
                if (currentPointers.get(currentPointers.size() -1)[0] == -1){
                    break;
                }
                currentNode = StorageManager.getBplusNode(treeId, currentPointers.get(currentPointers.size() -1)[0]);
                currentIndex = 0;
                currentPointers = currentNode.pointers;
            } else {
                currentIndex += 1;
            }
            //update with the next value
            currentValue = currentNode.values.get(currentIndex);
        }
    }
}
