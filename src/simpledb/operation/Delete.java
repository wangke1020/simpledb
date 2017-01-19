package simpledb.operation;

import simpledb.*;
import simpledb.struct.DbIterator;
import simpledb.struct.IntField;
import simpledb.struct.Tuple;
import simpledb.struct.TupleDesc;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator iterator;
    boolean deleted;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        tid = t;
        iterator = child;
        deleted = false;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
        super.open();
        deleted = false;
    }

    public void close() {
        iterator.close();
        super.close();
        deleted = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
        deleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(deleted) return null;
        int count = 0;
        while(iterator.hasNext()) {
            Database.getBufferPool().deleteTuple(tid, iterator.next());
            ++count;
        }
        deleted = true;
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
