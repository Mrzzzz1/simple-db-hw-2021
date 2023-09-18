package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    Map<Field,Integer> groupMap;
    Map<Field,Integer> countMap;
    Map<Field, List<Integer>> avgMap;

    TupleDesc td;
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
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
        this.avgMap = new HashMap<>();
        this.countMap = new HashMap<>();
        if(gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"aggregateVal"});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},new String[]{"groupVal","aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField field =(IntField) tup.getField(afield);
        Field gbField = this.gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        int value = field.getValue();
        switch (what){
            case MIN -> {
                if(!groupMap.containsKey(gbField)) {
                    groupMap.put(gbField,value);
                } else groupMap.put(gbField,Math.min(value,groupMap.get(gbField)));
            }
            case MAX -> {
                if(!groupMap.containsKey(gbField)) {
                    groupMap.put(gbField,value);
                } else groupMap.put(gbField,Math.max(value,groupMap.get(gbField)));
            }
            case SUM -> {
                if(!groupMap.containsKey(gbField)) {
                    groupMap.put(gbField,value);
                } else groupMap.put(gbField,groupMap.get(gbField)+value);
            }
            case COUNT -> {
                if(!groupMap.containsKey(gbField)) {
                    groupMap.put(gbField,1);
                } else groupMap.put(gbField,groupMap.get(gbField)+1);
            }
            case AVG -> {
                if(!avgMap.containsKey(gbField)) {
                    List<Integer> list = new ArrayList<>();
                    avgMap.put(gbField,list);
                    list.add(value);
                } else avgMap.get(gbField).add(value);
            }
            case SC_AVG -> {}
            case SUM_COUNT -> {}
            default -> {}
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            Iterator<Map.Entry<Field,Integer>> iterator1;
            Iterator<Map.Entry<Field,List<Integer>>> iterator2;





            @Override
            public void open() throws DbException, TransactionAbortedException {
                if(what.equals(Op.AVG)) iterator2 = avgMap.entrySet().iterator();
                else iterator1 = groupMap.entrySet().iterator();

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(what.equals(Op.AVG))return iterator2.hasNext();
                else return iterator1.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple t = new Tuple(td);
                if(what.equals(Op.AVG)) {
                    int avg = 0;
                    Map.Entry<Field, List<Integer>> entry = iterator2.next();
                    for (Integer integer : entry.getValue()) {
                        avg+=integer;
                    }
                    avg/=entry.getValue().size();
                    if(gbfield!=NO_GROUPING) {
                        t.setField(0,entry.getKey());
                        t.setField(1,new IntField(avg));
                    } else t.setField(0,new IntField(avg));
                }
                else {
                    Map.Entry<Field, Integer> entry = iterator1.next();
                    if(gbfield!=NO_GROUPING) {
                        t.setField(0,entry.getKey());
                        t.setField(1,new IntField(entry.getValue()));
                    } else t.setField(0,new IntField(entry.getValue()));

                }

                return t;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if(what.equals(Op.AVG)) iterator2 = avgMap.entrySet().iterator();
                else iterator1 = groupMap.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                iterator1 = null;
                iterator2 = null;
            }
        };

    }

}
