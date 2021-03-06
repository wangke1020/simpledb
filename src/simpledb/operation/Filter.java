package simpledb.operation;

import simpledb.*;
import simpledb.struct.DbIterator;
import simpledb.struct.Tuple;
import simpledb.struct.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate pred;
    private DbIterator dbIterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        pred = p;
        dbIterator = child;

    }

    public Predicate getPredicate() {
        return pred;
    }

    public TupleDesc getTupleDesc() {
        return dbIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        dbIterator.open();
        super.open();
    }

    public void close() {
        dbIterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        dbIterator.rewind();
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
        while(dbIterator.hasNext())
        {
            Tuple t = dbIterator.next();
            if(pred.filter(t))
                return t;
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{dbIterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        dbIterator = children[0];
    }

}
