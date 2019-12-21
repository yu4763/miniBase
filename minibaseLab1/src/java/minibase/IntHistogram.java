package minibase;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

	/**
	 * Create a new IntHistogram.
	 * 
	 * This IntHistogram should maintain a histogram of integer values that it
	 * receives. It should split the histogram into "buckets" buckets.
	 * 
	 * The values that are being histogrammed will be provided one-at-a-time through
	 * the "addValue()" function.
	 * 
	 * Your implementation should use space and have execution time that are both
	 * constant with respect to the number of values being histogrammed. For
	 * example, you shouldn't simply store every value that you see in a sorted
	 * list.
	 * 
	 * @param buckets The number of buckets to split the input value into.
	 * @param min     The minimum integer value that will ever be passed to this
	 *                class for histogramming
	 * @param max     The maximum integer value that will ever be passed to this
	 *                class for histogramming
	 */

	private int[] buckets;
	private int numBuckets;
	private int min;
	private int max;
	private int width;
	private int numTuples;

	public IntHistogram(int buckets, int min, int max) {
		// some code goes here
		this.buckets = new int[buckets];
		this.numBuckets = buckets;
		this.min = min;
		this.max = max;
		this.width =  (int) Math.ceil((double)(max - min + 1) / this.numBuckets );
		this.numTuples = 0;
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * 
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here
		int index = (v - this.min) / this.width;
		this.buckets[index]++;
		numTuples++;
	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * 
	 * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate of
	 * the fraction of elements that are greater than 5.
	 * 
	 * @param op Operator
	 * @param v  Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {

		// some code goes here
		
		int index = (v - this.min) / this.width;
		double selectivity = 0.0;

		switch (op) {
		
				
			case LIKE:
			
			case EQUALS:	
				
				if(v < min  || v > max)
					return 0;
				
				return  ( (double) buckets[index] /this.width ) / this.numTuples;
				
			case GREATER_THAN:
				
				if( v < min)
					return 1;
				else if (v > max)
					return 0;
				
				selectivity = 0.0;
				int w_max = (index + 1)*this.width;
				selectivity += ((w_max - v)*buckets[index]) / (double) this.width;
				for(int i=index+1; i<numBuckets; i++) {
					selectivity += (double) buckets[i] /this.width;
				}
				selectivity /= this.numTuples;
				
				return selectivity;
				
			case LESS_THAN:
				
				if( v < min)
					return 0;
				else if (v > max)
					return 1;
				
				selectivity = 0.0;
				int w_min = index*this.width;
				selectivity += ((v-w_min)*buckets[index]) / (double) this.width;
				for(int i=index-1; i>=0; i--) {
					selectivity += (double) buckets[i] /this.width;
				}
				selectivity /= this.numTuples;
				
				return selectivity;
				
			case LESS_THAN_OR_EQ:
				
				if( v < min)
					return 0;
				else if (v > max)
					return 1;
			
				selectivity = 0.0;
				for(int i=index; i>=0; i--) {
					selectivity += (double) buckets[i] /this.width;
				}
				selectivity /= this.numTuples;
				
				return selectivity;
				
				
			case GREATER_THAN_OR_EQ:
				
				if( v < min)
					return 1;
				else if (v > max)
					return 0;
				
				selectivity = 0.0;
				for(int i=index; i<numBuckets; i++) {
					selectivity += (double) buckets[i] /this.width;
				}
				selectivity /= this.numTuples;
				
				return selectivity;
	
				

			case NOT_EQUALS:
				
				if(v < min || v> max) 
					return 1;
				
				return 1 - ( (double) buckets[index] /this.width ) / this.numTuples;
				
			default:
				return -1.0;

		}

		
	}

	/**
	 * @return the average selectivity of this histogram.
	 * 
	 *         This is not an indispensable method to implement the basic join
	 *         optimization. It may be needed if you want to implement a more
	 *         efficient optimization
	 */
	public double avgSelectivity() {
		// some code goes here
		
		double selectivity = 0.0;
		for(int i=0; i<numBuckets; i++) {
			selectivity += buckets[i] / (double) this.width;
		}
		selectivity /= this.numTuples; 
		return selectivity;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {

		// some code goes here
		return null;
	}
}
