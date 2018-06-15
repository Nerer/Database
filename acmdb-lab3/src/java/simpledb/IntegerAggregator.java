package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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

    int gbfield, afield;
    Type gbfieldtype;
    Field gb;
    Op aop;
    HashMap<Field, Integer> res;
    HashMap<Field, Integer> gbCount;
    String gbname;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.aop = what;
        res = new HashMap<>();
        gbCount = new HashMap<>();
        gbname = null;
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
        if (gbfield == Aggregator.NO_GROUPING) {
            gb = null;
        } else {
            gb = tup.getField(gbfield);
            if (gbname == null) {
                gbname = tup.getTupleDesc().getFieldName(gbfield);
            }
        }
        int val = ((IntField)tup.getField(afield)).getValue();
        if (!res.containsKey(gb)) {
            res.put(gb, val);
        } else {
            if (aop == Op.AVG || aop == Op.SUM) {
                res.put(gb, val + res.get(gb));
            }
            if (aop == Op.MAX)  {
                res.put(gb, Math.max(val, res.get(gb)));
            }
            if (aop == Op.MIN) {
                res.put(gb, Math.min(val, res.get(gb)));
            }
        }

        if (gbCount.containsKey(gb)) {
            gbCount.put(gb, gbCount.get(gb) + 1);
        } else {
            gbCount.put(gb, 1);
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
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] tp = new Type[1];
            tp[0] = Type.INT_TYPE;
            String[] fn = new String[1];
            fn[0] = aop.toString();
            TupleDesc td = new TupleDesc(tp, fn);
            Tuple t = new Tuple(td);

            if (aop == Op.AVG) {
                t.setField(0, new IntField(res.get(null) / gbCount.get(null)));
            }
            if (aop == Op.MAX || aop == Op.MIN || aop == Op.SUM) {
                t.setField(0, new IntField(res.get(null)));
            }
            if (aop == Op.COUNT) {
                t.setField(0, new IntField(gbCount.get(null)));
            }
            ArrayList<Tuple> a = new ArrayList<>();
            a.add(t);
            return new TupleIterator(td, a);
        } else {
            Type[] tp = new Type[2];
            tp[0] = gbfieldtype;
            tp[1] = Type.INT_TYPE;
            String[] fn = new String[2];
            fn[0] = gbname;
            fn[1] = aop.toString();
            TupleDesc td = new TupleDesc(tp, fn);
            ArrayList<Tuple> a = new ArrayList<>();
            for (Field f : gbCount.keySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, f);
                if (aop == Op.AVG) {
                    t.setField(1, new IntField(res.get(f) / gbCount.get(f)));
                }
                if (aop == Op.MAX || aop == Op.MIN || aop == Op.SUM) {
                    t.setField(1, new IntField(res.get(f)));
                }
                if (aop == Op.COUNT) {
                    t.setField(1, new IntField(gbCount.get(f)));
                }
                a.add(t);
            }
            return new TupleIterator(td, a);
        }
    }

}
