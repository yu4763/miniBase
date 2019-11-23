package minibase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import minibase.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private HashMap<Field, Integer> results;
	private TupleDesc td;
	private int calc = 0;
	private int len = 0;
	private HashMap<Field, Integer> lens;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
	 *                    null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        the aggregation operator
	 */

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		this.results = new HashMap<Field, Integer>();
		this.lens = new HashMap<Field, Integer>();
	
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here

		if (this.td == null) {
			td = tup.getTupleDesc();
		}

		if (gbfield == Aggregator.NO_GROUPING) {
			if (what == Aggregator.Op.COUNT) {
				calc++;

			} else if (what == Aggregator.Op.MAX) {
				IntField f = (IntField) tup.getField(0);
				if (f.getValue() > calc) {
					calc = f.getValue();
				}
			} else if (what == Aggregator.Op.MIN) {

				IntField f = (IntField) tup.getField(0);
				if (f.getValue() < calc) {
					calc = f.getValue();
				}

			} else if (what == Aggregator.Op.AVG || what == Aggregator.Op.SUM) {
				IntField f = (IntField) tup.getField(0);
				calc += f.getValue();
				len++;
			}
		} else {

			Field gf = tup.getField(0);

			if (what == Aggregator.Op.COUNT) {
				results.computeIfPresent(gf, (k, v) -> v + 1);
				results.putIfAbsent(gf, 1);

			} else if (what == Aggregator.Op.MAX) {
				IntField f = (IntField) tup.getField(1);
				results.computeIfPresent(gf, (k, v) -> max(v, f.getValue()));
				results.putIfAbsent(gf, f.getValue());
				
				
			} else if (what == Aggregator.Op.MIN) {

				IntField f = (IntField) tup.getField(1);
				results.computeIfPresent(gf, (k, v) -> min(v, f.getValue()));
				results.putIfAbsent(gf, f.getValue());
				

			} else if (what == Aggregator.Op.AVG || what == Aggregator.Op.SUM) {
				IntField f = (IntField) tup.getField(1);
				results.computeIfPresent(gf, (k, v) -> v + f.getValue());
				results.putIfAbsent(gf, f.getValue());
				if(what == Aggregator.Op.AVG) {
					lens.computeIfPresent(gf, (k, v) -> v+1);
					lens.putIfAbsent(gf, 1);
				}
			}

		}

	}

	public int min(int a, int b) {

		if (a < b)
			return a;
		else
			return b;

	}

	public int max(int a, int b) {

		if (a > b)
			return a;
		else
			return b;

	}

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if
	 *         using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in the
	 *         constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		List<Tuple> newTuples = new ArrayList<Tuple>();

		if (gbfield == Aggregator.NO_GROUPING) {

			if (what == Aggregator.Op.AVG) {
				calc = calc / len;
			}

			Tuple tt = new Tuple(td);
			tt.setField(0, new IntField(calc));
			newTuples.add(tt);

			return new TupleIterator(td, newTuples);
		} else {

			for (Field f : results.keySet()) {

				Tuple tt = new Tuple(td);
				Field ff;

				if (what == Aggregator.Op.AVG) {
					
					ff = new IntField(results.get(f) / lens.get(f));
				} else {
					ff = new IntField(results.get(f));
				}

				tt.setField(0, f);
				tt.setField(1, ff);
				newTuples.add(tt);

			}
			
			return new TupleIterator(td, newTuples);

		}

	}

}
