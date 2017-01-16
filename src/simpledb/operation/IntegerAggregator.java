package simpledb.operation;

import simpledb.DbException;
import simpledb.TransactionAbortedException;
import simpledb.struct.*;
import simpledb.Type;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldPos;
    private Type gbFieldType;
    private int aggFieldPos;
    private Op op;
    private TupleDesc tupleDesc;

    HashMap<Field, Tuple> map;
    HashMap<Field, Integer> counts;
    HashMap<Field, Integer> sums;
//    Tuple noGroupingTuple;
//    int noGroupingCount;
//    int noGroupingSum;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbFieldPos = gbfield;
        gbFieldType = gbfieldtype;
        aggFieldPos = afield;
        op = what;

        map = new HashMap<>();
        if(what.equals(Op.AVG)) {
            counts = new HashMap<>();
            sums = new HashMap<>();
        }
        tupleDesc = new TupleDesc(gbfield == NO_GROUPING ?
                new Type[]{Type.INT_TYPE} : new Type[] {gbfieldtype, Type.INT_TYPE});

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField newField = (IntField)tup.getField(aggFieldPos);
        int newValue = newField.getValue();

        if(gbFieldPos == NO_GROUPING) {
            mergeNoGroupingTuple(tup, newField, newValue);
            return;
        }

        Field f = tup.getField(gbFieldPos);
        Tuple originTuple = map.get(f);

        if(originTuple != null) {

            Field aggField = originTuple.getField(aggFieldPos);
            Integer value = ((IntField) aggField).getValue();
            switch (op) {
                case COUNT:
                    originTuple.setField(1, new IntField(value+1));
                    break;
                case MAX:
                    if(newField.compare(Predicate.Op.GREATER_THAN, aggField))
                        originTuple.setField(1, newField);
                    break;
                case MIN:
                    if(newField.compare(Predicate.Op.LESS_THAN, aggField))
                        originTuple.setField(1, newField);
                    break;
                case SUM:
                    originTuple.setField(1, new IntField(value + newField.getValue()));
                    break;
                case AVG:
                    int sz = counts.get(f);
                    int sum = sums.get(f);
                    sum += newField.getValue();
                    int avg = sum / (++sz);
                    originTuple.setField(1, new IntField(avg));
                    counts.remove(f);
                    counts.put(f, sz);
                    sums.remove(f);
                    sums.put(f, sum);
                    break;
                default:
                    break;
            }
        }else {
            Tuple t = new Tuple(tupleDesc);
            t.setField(0, tup.getField(gbFieldPos));
            switch (op) {
                case COUNT:
                    t.setField(aggFieldPos, new IntField(1));
                    break;
                case AVG:
                    counts.put(f, 1);
                    sums.put(f, ((IntField) tup.getField(aggFieldPos)).getValue());
                    t.setField(1, tup.getField(aggFieldPos));
                    break;
                case MAX:
                case MIN:
                case SUM:
                    t.setField(1, tup.getField(aggFieldPos));
                default:
                    break;
            }
            map.put(f, t);
        }
    }

    private void mergeNoGroupingTuple(Tuple tup, IntField newField, int newValue) {
        IntField fakeField = new IntField(0);
        if(map.size() == 0) {
            Tuple t = new Tuple(tupleDesc);
            map .put(fakeField, t);
            switch (op) {
                case COUNT:
                    t.setField(aggFieldPos, new IntField(1));
                    break;
                case AVG:
                    counts.put(fakeField, 1);
                    sums.put(fakeField, newValue);
                case MAX:
                case MIN:
                case SUM:
                    t.setField(1, tup.getField(aggFieldPos));
                default:
                    break;
            }
        }
        else {
            Tuple t = map.get(fakeField);
            IntField oldAggField = (IntField) t.getField(1);
            int oldAggValue = oldAggField.getValue();
            IntField newAggField = null;
            switch (op) {
                case COUNT:
                    newAggField = new IntField(oldAggValue + 1);
                    break;
                case AVG:
                    int sum = sums.get(fakeField);
                    sum += newValue;
                    int count = counts.get(fakeField);
                    count += 1;

                    newAggField = new IntField(sum / (count));
                    sums.remove(fakeField);
                    sums.put(fakeField, sum);
                    counts.remove(fakeField);
                    counts.put(fakeField, count);
                    break;
                case MAX:
                    if (newValue > oldAggValue)
                        newAggField = newField;
                    break;
                case MIN:
                    if (newValue < oldAggValue)
                        newAggField = newField;

                    break;
                case SUM:
                    newAggField = new IntField(newValue + oldAggValue);
                    break;
                default:
                    break;
            }
            t.setField(aggFieldPos, newAggField);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        return Aggregator.makeIteratorFromMap(map, tupleDesc);
    }

}
