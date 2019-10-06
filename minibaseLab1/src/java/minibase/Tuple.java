package minibase;

import java.io.Serializable;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    List <Field> fieldVal;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // TODO: some code goes here
    	this.tupleDesc = td;
		this.fieldVal = new ArrayList<>();
		int len = td.numFields();
		for(int i=0; i<len; i++){
			this.fieldVal.add(null);
		}
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
    	return this.tupleDesc;

    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // TODO: some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // TODO: some code goes here
    	this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // TODO: some code goes here
    	if(i<0 || i>=fieldVal.size()){
			return;
		}
		fieldVal.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // TODO: some code goes here
      
        if(i<0 || i>=fieldVal.size()){
			return null;
		}
		return fieldVal.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // TODO: some code goes here
    	String result = "";
		int len = this.tupleDesc.numFields();
		for(int i=0; i<len; i++){
			if(fieldVal.get(i) == null){
				result+="null\t";
			}
			else{
				if(fieldVal.get(i).getType() == Type.STRING_TYPE){
					StringField sf = (StringField) fieldVal.get(i);
					result += sf.getValue() + '\t';
				}
				else{
					IntField f = (IntField) fieldVal.get(i);
					result += Integer.toString(f.getValue() )+ '\t';
				}
			}
		}
		
		return result;
		
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fieldVal.iterator();
    }

}
