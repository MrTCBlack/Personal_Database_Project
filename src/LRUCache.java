import java.util.*;

/**
 * Implements a Least Recently Used (LRU) cache for managing pages in memory.
 * Uses a hashmap for quick lookups and a doubly linked list for maintaining usage order.
 * 
 * @author Justin Talbot, jmt8032@rit.edu
 * @author Brayden Mossey, bjm9599@rit.edu
 */
public class LRUCache {

    /**
     * Represents a node in the doubly linked list.
     * Stores a page and its key.
     */
    static class Node {
        int key;
        Object page;
        Node prev, next;

        Node(int key, Object page) {
            this.key = key;
            this.page = page;
        }
    }

    private final Node head, tail; // Pointers to the LRU list
    private final Map<Integer, Node> cache; // Map for fast lookups
    private final int capacity; // Maximum cache size

    /**
     * Constructor for LRU cache
     * @param capacity the capacity of the cache
     */
    public LRUCache(int capacity) {
        this.head = new Node(-1, null);
        this.tail = new Node(-1, null);
        this.capacity = capacity;
        this.cache = new HashMap<>();
        head.next = this.tail;
        tail.prev = this.head;
    }

    /**
     * Moves a node to the front of the list (most recently used).
     */
    private void moveToFront(Node node) {
        removeNode(node);
        insertAtFront(node);
    }

    /**
     * Removes a node from the list.
     */
    private void removeNode(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    /**
     * Inserts a node at the front of the list (most recently used).
     */
    private void insertAtFront(Node node) {
        node.next = this.head.next;
        node.prev = this.head;
        this.head.next.prev = node;
        this.head.next = node;
    }

    /**
     * Retrieves a page from the cache.
     * If the page exists, it is marked as recently used.
     */
    public Object get(int key) {
        Node node = cache.get(key);
        if (node == null) {
            return null;
        }
        moveToFront(node);
        return node.page;
    }

    /**
     * Inserts a page into the cache. Evicts the least recently used page if necessary.
     */
    public Object put(int key, Object page) {
        Node node = cache.get(key);
        Object output = null;

        if (node != null) {
            node.page = page;
            moveToFront(node);
        } else {
            if (cache.size() == capacity) {
                int lruKey = getLRUKey();
                if (lruKey != -1) {
                    cache.remove(lruKey);
                    output = tail.prev.page;
                    removeNode(tail.prev);
                }
            }

            Node newNode = new Node(key, page);
            cache.put(key, newNode);
            insertAtFront(newNode);
        }

        return output;
    }

    /**
     * Gets the least recently used page's key.
     */
    public int getLRUKey() {
        if (tail.prev == head) {
            return -1;
        }
        return tail.prev.key;
    }

    /**
     * Removes a page from the cache.
     */
    public Object remove(int key) {
        Node node = cache.get(key);
        if (node == null) {
            return null;
        }
        removeNode(node);
        cache.remove(key);
        return node.page;
    }

    /**
     * Returns the current number of pages in the cache.
     */
    public int size() {
        return cache.size();
    }
}
