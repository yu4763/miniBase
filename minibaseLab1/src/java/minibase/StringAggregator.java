package minibase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private HashMap<Field, Integer> results;
	private int cnts;
	private TupleDesc td;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
	 *                    null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here

		if (what != Op.COUNT) {
			throw new IllegalArgumentException();
		}

		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		this.results = new HashMap<Field, Integer>();
		this.cnts = 0;
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
		if(this.td == null) {
			td = tup.getTupleDesc();
		}
		if(gbfield == Aggregator.NO_GROUPING ) {
			cnts++;
			return;
		}
		else {
			
			Field f = tup.getField(0);
			results.computeIfPresent(f, (k, v) -> v+1);
			results.putIfAbsent(f, 1);
			
		}

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
		
		if(gbfield == Aggregator.NO_GROUPING ) {
		
			Tuple tt = new Tuple(td);
			tt.setField(0, new IntField(cnts));
			newTuples.add(tt);
			
			return new TupleIterator(td, newTuples);
		}
		else {
			
		
			for(Field f: results.keySet()) {
				
				Tuple tt = new Tuple(td);
				Field ff = new IntField(results.get(f));
				tt.setField(0, f);
				tt.setField(1, ff);
				newTuples.add(tt);
				
			}
			
			return new TupleIterator(td, newTuples);
	
			
		}

		
	}

}
