package simpledb.operation;

import simpledb.DbException;
import simpledb.TransactionAbortedException;
import simpledb.Type;
import simpledb.struct.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    static final int NO_GROUPING = -1;

    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "counts";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a DbIterator over group aggregate results.
     * @see TupleIterator for a possible helper
     */
    public DbIterator iterator();


    static DbIterator makeIteratorFromMap(HashMap<Field, Tuple> map, TupleDesc td) {
        return new DbIterator() {
            private Iterator<Tuple> iterator;
            private boolean opened;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                iterator = map.values().iterator();
                opened = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!opened)
                    throw new IllegalStateException("not opened");
                return iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!opened)
                    throw new IllegalStateException("not opened");
                if(!hasNext())
                    throw new NoSuchElementException("no more element");
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if(!opened)
                    throw new IllegalStateException("not opened");
                iterator = map.values().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                opened = false;
            }
        };
    }
    
}
