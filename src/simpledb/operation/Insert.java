package simpledb.operation;

import simpledb.*;
import simpledb.struct.DbIterator;
import simpledb.struct.IntField;
import simpledb.struct.Tuple;
import simpledb.struct.TupleDesc;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator iterator;
    private TransactionId tid;
    private int tableId;
    private boolean inserted;


    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        iterator = child;
        tid = t;
        tableId = tableid;
        inserted = false;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
        super.open();
        inserted = false;
    }

    public void close() {
        inserted = true;
        iterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(inserted) return null;
        int count = 0;
        while(iterator.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableId, iterator.next());
            } catch (IOException e) {
                e.printStackTrace();
            }
            ++count;
        }
        inserted = true;
        Tuple t = new Tuple(getTupleDesc());
        t.setField(0, new IntField(count));

        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{iterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        iterator = children[0];
    }
}
