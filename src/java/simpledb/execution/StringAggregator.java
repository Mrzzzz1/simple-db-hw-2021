package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    Map<Field,Integer> groupMap;
    TupleDesc td;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (!what.equals(Op.COUNT)) throw new IllegalArgumentException();
        groupMap = new HashMap<>();
        if(gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"aggregateVal"});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},new String[]{"groupVal","aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField  = this.gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        if(!groupMap.containsKey(gbField)) {
            groupMap.put(gbField,1);
        } else groupMap.put(gbField,groupMap.get(gbField)+1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            Iterator<Map.Entry<Field,Integer>> iterator;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                iterator = groupMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple t = new Tuple(td);
                Map.Entry<Field, Integer> entry = iterator.next();
                if(gbfield == NO_GROUPING) {
                    t.setField(0,new IntField(entry.getValue()));
                } else {
                    t.setField(0, entry.getKey());
                    t.setField(1,new IntField(entry.getValue()));
                }
                return t;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iterator = groupMap.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                iterator = null;
            }
        };
    }

}
