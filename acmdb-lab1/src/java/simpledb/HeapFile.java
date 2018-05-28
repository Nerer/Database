package simpledb;

import sun.reflect.annotation.ExceptionProxy;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc tupleDesc;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile br = new RandomAccessFile(this.file, "r");
            int offset = BufferPool.getPageSize() * pid.pageNumber();
            br.seek(offset);
            byte[] data = new byte[BufferPool.getPageSize()];
            //System.out.println(Integer.toString((int)br.length()) + " " + offset + " " + BufferPool.getPageSize());
            br.read(data, 0, BufferPool.getPageSize());
            return new HeapPage((HeapPageId)pid, data);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try {
            RandomAccessFile br = new RandomAccessFile(this.file, "r");
            long len = br.length();
            return (int)len/BufferPool.getPageSize();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public class HeapFileIterator implements DbFileIterator {
        int curPageNo;
        int tableId;
        boolean stats;
        int maxPages;
        TransactionId tid;
        Iterator<Tuple> itInPage;
        public int getcur() {
            return curPageNo;
        }
        public int getmax() {return maxPages; }
        public HeapFileIterator(TransactionId transactionId) {
            curPageNo = 0;
            tableId = HeapFile.this.getId();
            tid = transactionId;
            maxPages = numPages();
            itInPage = null;
            stats = false;
        }
        public boolean hasNext() {
            if (stats == false) {
                return false;
            }
            if (curPageNo >= maxPages) {
                return false;
            }
            //System.out.println(numPages());
            if (itInPage == null) {
                try {
                    PageId nextPageId = new HeapPageId(tableId, curPageNo);
                    //System.out.println(tableId);
                    //System.out.println(curPageNo);
                    HeapPage tempPage = (HeapPage) Database.getBufferPool().getPage(tid, nextPageId, Permissions.READ_ONLY);
                    /*if (tempPage == null) {
                        System.out.println("oh no");
                    }*/
                    itInPage = tempPage.iterator();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (itInPage.hasNext()) {
                return true;
            } else {
                if (curPageNo < maxPages - 1) {
                    try {
                        int nextPageNo = curPageNo + 1;
                        PageId nextPageId = new HeapPageId(HeapFile.this.getId(), nextPageNo);
                        HeapPage tempPage = (HeapPage) Database.getBufferPool().getPage(tid, nextPageId, Permissions.READ_ONLY);

                        while (tempPage.iterator().hasNext() == false) {
                            if (nextPageNo < maxPages - 1) {
                                nextPageNo++;
                                nextPageId = new HeapPageId(HeapFile.this.getId(), nextPageNo);
                                tempPage = (HeapPage) Database.getBufferPool().getPage(tid, nextPageId, Permissions.READ_ONLY);
                            } else {
                                return false;
                            }
                        }
                        return tempPage.iterator().hasNext();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    return false;
                }
            }

            return false;
        }
        public Tuple next() throws NoSuchElementException{
            if (hasNext()) {
                if (itInPage.hasNext()) {
                    return itInPage.next();
                } else {
                    int nextPageNo = curPageNo + 1;
                    PageId nextPageId = new HeapPageId(HeapFile.this.getId(), nextPageNo);
                    try {
                        HeapPage tempPage = (HeapPage) Database.getBufferPool().getPage(tid, nextPageId, Permissions.READ_ONLY);
                        while (tempPage.iterator().hasNext() == false) {
                            nextPageNo++;
                            nextPageId = new HeapPageId(HeapFile.this.getId(), nextPageNo);
                            tempPage = (HeapPage) Database.getBufferPool().getPage(tid, nextPageId, Permissions.READ_ONLY);
                        }
                        curPageNo = nextPageNo;
                        itInPage = tempPage.iterator();
                        return itInPage.next();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else {
                throw new NoSuchElementException("No next");
            }
            return null;
        }
        public void close() {
            stats = false;
        }
        public void open() {
            stats = true;
        }
        public void rewind() throws DbException{
            if (stats == false) {
                throw new DbException("Rewind not supported because the iterator is closed");
            }
            curPageNo = 0;
            //tableId = HeapFile.this.getId();
            itInPage = null;
        }
    }
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
        //return null;
    }

}

