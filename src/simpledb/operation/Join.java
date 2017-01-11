package simpledb.operation;

import simpledb.*;
import simpledb.struct.DbIterator;
import simpledb.struct.Tuple;
import simpledb.struct.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate joinPred;
    private DbIterator leftChild;
    private DbIterator rightChild;

    private Tuple leftCurTuple;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        joinPred = p;
        leftChild = child1;
        rightChild = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return joinPred;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return leftChild.getTupleDesc().getFieldName(joinPred.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return rightChild.getTupleDesc().getFieldName(joinPred.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(leftChild.getTupleDesc(), rightChild.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        leftChild.open();
        rightChild.open();
        super.open();
    }

    public void close() {
        leftChild.close();
        rightChild.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        leftChild.rewind();
        rightChild.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(!leftChild.hasNext() && !rightChild.hasNext())
            return null;
        if(leftCurTuple == null) {
            leftCurTuple = leftChild.next();
        }
        if(rightChild.hasNext()) {
            Tuple rightNext = rightChild.next();
            if (joinPred.filter(leftCurTuple, rightNext))
                return merge(leftCurTuple, rightNext);
            else
                return fetchNext();
        }else {
            leftCurTuple = null;
            rightChild.rewind();
            return fetchNext();
        }
    }

    private Tuple merge(Tuple left, Tuple right) {
        Tuple t = new Tuple(getTupleDesc());
        int i =0;

        int leftLen = leftChild.getTupleDesc().numFields();
        for(; i< leftLen; ++i)
            t.setField(i, left.getField(i));
        for(int j=0;j < rightChild.getTupleDesc().numFields(); ++j) {
            t.setField(i+j, right.getField(j));
        }
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{leftChild, rightChild};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        leftChild = children[0];
        rightChild = children[2];
    }

}
