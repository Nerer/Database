package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    Predicate pred;
    DbIterator iter;
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        pred = p;
        iter = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return iter.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        iter.open();
    }

    public void close() {
        // some code goes here
        super.close();
        iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        iter.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple ret = null;
        while (iter.hasNext()) {
            ret = iter.next();
            if (pred.filter(ret)) {
                return ret;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] a;
        a = new DbIterator[1];
        a[0] = iter;
        return a;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        iter = children[0];
    }

}
