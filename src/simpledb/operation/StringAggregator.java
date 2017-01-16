package simpledb.operation;

import simpledb.struct.*;
import simpledb.Type;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldPos;
    private Type gbFieldType;
    private int aggFieldPos;
    private TupleDesc tupleDesc;
    private Op op;

    HashMap<Field, Tuple> map = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(!what.equals(Op.COUNT))
            throw new IllegalArgumentException("not supported operation: " + what.toString());

        gbFieldPos = gbfield;
        gbFieldType = gbfieldtype;
        aggFieldPos = afield;

        tupleDesc = new TupleDesc(gbfield == NO_GROUPING ?
        new Type[]{Type.INT_TYPE} : new Type[] {gbfieldtype, Type.INT_TYPE});
        op = what;

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbFieldPos == NO_GROUPING) {
            mergeNoGroupingTuple();
        }
        else {
            Field key = tup.getField(gbFieldPos);
            Tuple aggTuple = map.get(key);
            if(aggTuple == null) {
                Tuple t = new Tuple(tupleDesc);
                t.setField(0, tup.getField(gbFieldPos));
                t.setField(1, new IntField(1));
                map.put(key, t);
            }else {
                int oldCount = ((IntField)aggTuple.getField(1)).getValue();
                aggTuple.setField(1, new IntField(oldCount+1));
            }
        }
    }

    private void mergeNoGroupingTuple() {
        IntField fakeFiled = new IntField(0);
        if(map.size() == 0) {
            Tuple t= new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            t.setField(0, new IntField(1));
            map.put(fakeFiled, t);
        }else {
            Tuple aggTuple = map.get(fakeFiled);
            IntField aggField = (IntField) aggTuple.getField(0);
            aggTuple.setField(0, new IntField(aggField.getValue()+1));
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return Aggregator.makeIteratorFromMap(map, tupleDesc);
    }

}
