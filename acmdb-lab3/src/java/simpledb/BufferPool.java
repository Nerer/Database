package simpledb;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    private Page[] pages;
    private HashMap<PageId, Page> pageMap;
    private int maxPages;
    private int curPages;
    private Lock getPageLock;
    private LinkedList<PageId> LRU;
    private Boolean[] empty;
    public BufferPool(int numPages) {
        // some code goes here
        maxPages = numPages;
        pages = new Page[numPages];
        pageMap = new HashMap<>();
        LRU = new LinkedList<>();
        curPages = 0;
        empty = new Boolean[numPages];
        for (int i = 0; i < numPages; i++) {
            empty[i] = true;
        }
        getPageLock = new ReentrantLock();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }


    public int getCur() {
        return curPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */


    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        getPageLock.lock();

        try {
            /*for (int i = 0; i < maxPages; i++) {
                if (empty[i] == false && pages[i].getId().equals(pid)) {
                    LRU.remove(LRU.indexOf(i));
                    LRU.add(i);
                    return pages[i];
                }
            }*/
            if (pageMap.containsKey(pid)) {
                LRU.remove(pid);
                LRU.add(pid);
                return pageMap.get(pid);
            }
            if (pageMap.size() == maxPages) {
                evictPage();
            }

            int tableId = pid.getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tableId);
            Page page = file.readPage(pid);
            LRU.add(pid);
            pageMap.put(pid, page);
            //insertPage(page);
            return page;
        } catch(Exception ex) {
            if (ex instanceof DbException) {
                throw new DbException("GG");
            }
            if (ex instanceof NoSuchElementException) {
                ex.printStackTrace();
                throw new DbException("No required Table");
            }
        } finally {
            getPageLock.unlock();
        }
        return null;
    }

    /*private void insertPage(Page page) {
        curPages++;
        for (int i = 0; i < maxPages; i++) {
            if (empty[i]) {
                LRU.add(i);
                pages[i] = page;
                empty[i] = false;
                break;
            }
        }
    }*/
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */

    private boolean equal(PageId a, PageId b) {
        //return a.getTableId() == b.getTableId() && a.pageNumber() == b.pageNumber();
        return a.equals(b);
    }

    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirty = file.insertTuple(tid, t);
        /*for (Page page : dirty) {
            PageId pid = page.getId();
            boolean found = false;
            int pos = -1;
            for (int i = 0; i < maxPages; i++) {
                if (!empty[i] && equal(pages[i].getId(),pid)) {
                    found = true;
                    pos = i;
                    break;
                }
            }
            if (found) {
                page.markDirty(true, tid);
                pages[pos] = page;
                pages[pos].markDirty(true, tid);
            } else {
                if (curPages == maxPages) {
                    evictPage();
                }
                page.markDirty(true, tid);
                insertPage(page);
                //page.markDirty(true, tid);
            }
        }*/
        for (Page page : dirty) {
            PageId pid = page.getId();
            //System.out.println(pid + " "+ ((BTreePage)page).getNumEmptySlots());
            /*if (page.getId().pageNumber() == 2) {
                System.out.println(((BTreePage)page).getNumEmptySlots());
            }*/
            if (pageMap.containsKey(pid)) {
                pageMap.get(pid).markDirty(true, tid);
               // LRU.remove(pid);
               // LRU.add(pid);
            } else {
                if (pageMap.size() == maxPages) {
                    evictPage();
                }
                pageMap.put(pid, page);
                LRU.remove(pid);
                LRU.add(pid);
                pageMap.get(pid).markDirty(true, tid);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirty = file.deleteTuple(tid, t);
        /*for (Page page : dirty) {
            PageId pid = page.getId();
            boolean found = false;
            int pos = -1;
            for (int i = 0; i < maxPages; i++) {
                if (!empty[i] && equal(pages[i].getId(),pid)) {
                    found = true;
                    pos = i;
                    break;
                }
            }
            if (found) {
                page.markDirty(true, tid);
                pages[pos] = page;
                pages[pos].markDirty(true, tid);
            } else {
                if (curPages == maxPages) {
                    evictPage();
                }
                page.markDirty(true, tid);
                insertPage(page);
            }
        }*/
        for (Page page : dirty) {
            PageId pid = page.getId();
            if (pageMap.containsKey(pid)) {
                pageMap.get(pid).markDirty(true, tid);
            } else {
                if (pageMap.size() == maxPages) {
                    evictPage();
                }
                pageMap.put(pid, page);
                LRU.remove(pid);
                LRU.add(pid);
                pageMap.get(pid).markDirty(true, tid);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
     //   System.out.println("123123");
        // not necessary for lab1
        /*for (int i = 0; i < maxPages; i++) {
            if (!empty[i] && pages[i].isDirty() != null) {
                flushPage(pages[i].getId());
            }
        }*/
        for (PageId pid : pageMap.keySet()) {
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        /*for (int i = 0; i < maxPages; i++) {
            if (!empty[i] && equal(pages[i].getId(), pid)) {
                empty[i] = true;
                curPages--;
                break;
            }
        }*/
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //System.out.println("flush");
        /*Page page = null;
        //System.out.println(pid);
        for (int i = 0; i < maxPages; i++) {
            if (!empty[i] && equal(pages[i].getId(), pid)) {
                page = pages[i];
            }
        }
        if (page != null && page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }*/
        Page page = pageMap.get(pid);
        if (page != null && page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //curPages--;
        //empty[temp] = true;
        /*curPages--;
        int temp = -1;
        for (int i : LRU) {
            if (pages[i].isDirty() == null) {
                temp = i;
            }
        }
        if (temp == -1) {
            temp = LRU.getFirst();
        }
        try {
            flushPage(pages[temp].getId());
        } catch (Exception e){
            e.printStackTrace();
        }
        LRU.remove(LRU.indexOf(temp));
        empty[temp] = true;*/
        PageId toEvict = null;
        for (PageId pid : LRU) {
            if (pageMap.get(pid).isDirty() == null) {
                toEvict = pid;
                break;
            }
        }
        if (toEvict == null) {
            toEvict = LRU.getFirst();
        }
        try {
            flushPage(toEvict);
        } catch (Exception e){
            e.printStackTrace();
        }
        LRU.remove(toEvict);
        pageMap.remove(toEvict);

    }

}
