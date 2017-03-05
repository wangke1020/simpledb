package simpledb.query.optimizer;

import simpledb.*;
import simpledb.operation.Predicate;
import simpledb.struct.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private static Object[] histograms;
    private int scanCost;
    private int tupleNum = 0;
    private TupleDesc td;

    class IntColumnValues {
        int max;
        int min;
        ArrayList<Integer> values = new ArrayList<>();

        IntColumnValues(int initValue) {
            max = initValue;
            min = initValue;
//            System.out.println("init: max=" + max + ",min=" + min);
        }

        void update(int v) {
            values.add(v);
            if(v > max) max = v;
            if(v < min) min = v;
//            System.out.println("v=" + v + ",max=" + max + ",min=" + min);
        }
        int maxVal() {
            return max;
        }
        int minVal() {
            return min;
        }

        ArrayList<Integer> getValues() {
            return values;
        }
    }

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {

        DbFile dbFile = Database.getCatalog().getDbFile(tableid);
        HeapFile heapFile = (HeapFile) dbFile;
        int pageNum = heapFile.numPages();
        this.scanCost = ioCostPerPage * pageNum;

        this.td = heapFile.getTupleDesc();
        histograms = new Object[td.numFields()];
        for(int i=0;i<td.numFields(); ++i) {
            if(td.getFieldType(i) == Type.STRING_TYPE) {
                histograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }

        Map<Integer, IntColumnValues> map = new HashMap<>();
        DbFileIterator iter = heapFile.iterator(new TransactionId());
        try {
            iter.open();
            while (iter.hasNext()) {
                ++tupleNum;
                Tuple t = iter.next();
                for(int i =0;i<td.numFields();++i) {
                    Object histogramObj = histograms[i];
                    Field field = t.getField(i);
                    if(td.getFieldType(i) == Type.INT_TYPE) {
                        int value = ((IntField) t.getField(i)).getValue();
                        if(map.get(i) == null) {
                            map.put(i, new IntColumnValues(value));
                        }else {
                            map.get(i).update(value);
                        }
                    }
                    else
                        ((StringHistogram)histogramObj).addValue(((StringField) field).getValue());
                }
            }
        }catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }

        for (Integer index : map.keySet()) {
            IntColumnValues intColValues = map.get(index);
            int range = intColValues.maxVal() - intColValues.minVal() + 1;
            IntHistogram intHistogram = new IntHistogram((int)Math.sqrt((double)range),
                    intColValues.minVal(), intColValues.maxVal());
            histograms[index] = intHistogram;
            intColValues.getValues().forEach(intHistogram::addValue);
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return scanCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(tupleNum * selectivityFactor);

    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        Type type = td.getFieldType(field);
        double sum = 0;
        int min = 0;
        int max = 0;
        if(type == Type.INT_TYPE) {
            IntHistogram histogram = (IntHistogram) histograms[field];
            min = histogram.minVal();
            max = histogram.maxVal();

            for(int i=min;i<=sum;++i) {
                sum += estimateSelectivity(field, op, new IntField(i));
            }

        }else {
            StringHistogram histogram = (StringHistogram) histograms[field];
            min = histogram.minVal();
            max = histogram.maxVal();
            for(int i=min;i<=sum;++i) {
                sum += estimateSelectivity(field, op, new IntField(i));
            }
        }

        return sum / (max - min + 1);
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        Type type = td.getFieldType(field);
        if(type == Type.INT_TYPE) {
            IntHistogram histogram = (IntHistogram) histograms[field];
            return histogram.estimateSelectivity(op, ((IntField)constant).getValue());
        }else {
            StringHistogram histogram = (StringHistogram) histograms[field];
            return histogram.estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return tupleNum;
    }

}
