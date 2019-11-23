package minibase;

import java.util.*;

import com.sun.management.VMOption.Origin;

import minibase.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

	private static final long serialVersionUID = 1L;
	private TupleDesc td;
	private Aggregator agg;
	private DbIterator child;
	private DbIterator[] children;
	private DbIterator results;
	private int afield;
	private int gfield;
	private Aggregator.Op aop;

	/**
	 * Constructor.
	 * 
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntAggregator} or {@link StringAggregator} to help you
	 * with your implementation of readNext().
	 * 
	 * 
	 * @param child  The DbIterator that is feeding us tuples.
	 * @param afield The column over which we are computing an aggregate.
	 * @param gfield The column over which we are grouping the result, or -1 if
	 *               there is no grouping
	 * @param aop    The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here

		this.td = child.getTupleDesc();
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;

		if (td.getFieldType(afield) == Type.INT_TYPE) {
			if (gfield == Aggregator.NO_GROUPING)
				agg = new IntegerAggregator(gfield, null, afield, aop);
			else
				agg = new IntegerAggregator(gfield, td.getFieldType(gfield), afield, aop);
		} else {
			if(gfield == Aggregator.NO_GROUPING)
				agg = new StringAggregator(gfield, null, afield, aop);
			else
				agg = new StringAggregator(gfield, td.getFieldType(gfield), afield, aop);
		}

		children = new DbIterator[1];
		children[0] = child;

	}

	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 *         field index in the <b>INPUT</b> tuples. If not, return
	 *         {@link minibase.Aggregator#NO_GROUPING}
	 */
	public int groupField() {
		// some code goes here
		return gfield;
	}

	/**
	 * @return If this aggregate is accompanied by a group by, return the name of
	 *         the groupby field in the <b>OUTPUT</b> tuples If not, return null;
	 */
	public String groupFieldName() {
		// some code goes here

		if (gfield == Aggregator.NO_GROUPING) {
			return null;
		}

		else {
			return td.getFieldName(gfield);
		}

	}

	/**
	 * @return the aggregate field
	 */
	public int aggregateField() {
		// some code goes here
		return afield;
	}

	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
	 */
	public String aggregateFieldName() {
		// some code goes here
		return td.getFieldName(afield);
	}

	/**
	 * @return return the aggregate operator
	 */
	public Aggregator.Op aggregateOp() {
		// some code goes here
		return aop;
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}

	public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
		// some code goes here
		super.open();
		child.open();

		while (child.hasNext()) {

			TupleDesc newTd = this.getTupleDesc();
			Tuple tt = new Tuple(newTd);
			Tuple origin = child.next();
			if (gfield == Aggregator.NO_GROUPING) {
				tt.setField(0, origin.getField(afield));
			} else {
				tt.setField(0, origin.getField(gfield));
				tt.setField(1, origin.getField(afield));
			}

			agg.mergeTupleIntoGroup(tt);
		}

		child.close();

		this.results = agg.iterator();
		results.open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first field is
	 * the field by which we are grouping, and the second field is the result of
	 * computing the aggregate, If there is no group by field, then the result tuple
	 * should contain one field representing the result of the aggregate. Should
	 * return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (results.hasNext()) {
			Tuple t = results.next();
			return t;
		}

		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		results.rewind();
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field, this
	 * will have one field - the aggregate column. If there is a group by field, the
	 * first field will be the group by field, and the second will be the aggregate
	 * value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given
	 * in the constructor, and child_td is the TupleDesc of the child iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		String gname = this.groupFieldName();
		TupleDesc newtd;
		Type[] types;
		String[] names;
		if (gname != null) {
			types = new Type[2];
			types[0] = td.getFieldType(gfield);
			types[1] = Type.INT_TYPE;
			names = new String[2];
			names[0] = gname;
			names[1] = nameOfAggregatorOp(aop) + "(" + this.aggregateFieldName() + ")";
			newtd = new TupleDesc(types, names);

		} else {
			types = new Type[1];
			types[0] = Type.INT_TYPE;
			names = new String[1];
			names[0] = nameOfAggregatorOp(aop) + "(" + this.aggregateFieldName() + ")";
			newtd = new TupleDesc(types, names);

		}

		return newtd;
	}

	public void close() {
		// some code goes here
		super.close();
		results.close();
	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return children;
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		this.children = children;
	}

}
