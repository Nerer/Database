package simpledb;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */

    TransactionId transactionId;
    int tableId;
    String tAlias;
    DbFileIterator iter;
    DbFile file;
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        transactionId = tid;
        tableId = tableid;
        tAlias = tableAlias;
        file = Database.getCatalog().getDatabaseFile(tableId);
        iter = file.iterator(tid);
        /*try {
            iter.open();
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        stats = false;
    }

    public HeapFile.HeapFileIterator getIt() {
        return (HeapFile.HeapFileIterator)iter;
    }
    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }
    public DbFile getFile() {
        return file;
    }
    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tAlias;
        //return null;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        tableId = tableid;
        tAlias = tableAlias;
        // some code goes here
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    boolean stats;
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        stats = true;
        iter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        HeapFile hf = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
        TupleDesc temp = hf.getTupleDesc();
        int len = temp.numFields();
        String[] fieldNames = new String[len];
        Type[] fieldTypes = new Type[len];
        for (int i = 0; i < temp.numFields(); i++) {
            fieldNames[i] = tAlias + "." + temp.getFieldName(i);
            fieldTypes[i] = temp.getFieldType(i);
        }
        return new TupleDesc(fieldTypes, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (!stats) {
            throw new IllegalStateException("Iterator is closed");
        }
        return iter.hasNext();
        // some code goes here
        //return false;
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (!stats) {
            throw new IllegalStateException("Iterator is closed");
        }
        if (hasNext()) {
            return iter.next();
        } else {
            throw new NoSuchElementException("No next");
        }
    }

    public void close() {
        iter.close();
        stats = false;
        // some code goes here
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        if (!stats) {
            throw new IllegalStateException("Iterator is closed");
        }
        iter.rewind();
        // some code goes here
    }
}
