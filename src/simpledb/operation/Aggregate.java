package simpledb.operation;

import simpledb.*;
import simpledb.struct.DbIterator;
import simpledb.struct.Tuple;
import simpledb.struct.TupleDesc;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private int aggField;
    private int groupField;
    private Aggregator.Op op;

    private DbIterator child;
    private TupleDesc td;
    private DbIterator aggIterator;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {

        aggField = afield;
        groupField = gfield;
        op = aop;
        this.child = child;
        td = child.getTupleDesc();
        Type type = td.getFieldType(afield);
        Aggregator aggregator;
        if(type == Type.INT_TYPE)
            aggregator = new IntegerAggregator(gfield, type, aggField, aop);
        else{
            aggregator = new StringAggregator(gfield, type, aggField, aop);
        }

        try {
            child.open();
            while (child.hasNext()) {
                aggregator.mergeTupleIntoGroup(child.next());
            }
        }catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }
        aggIterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        return td.getFieldName(groupField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return td.getFieldName(aggField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        aggIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(aggIterator.hasNext())
            return aggIterator.next();
        else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
        aggIterator.close();
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    child = children[0];
    }

}
