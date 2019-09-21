package minibase;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	/**
	 * A help class to facilitate organizing the information of each field
	 * */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 * */
		Type fieldType;

		/**
		 * The name of the field
		 * */
		String fieldName;

		public TDItem(Type t, String n) {
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	/**
	 * @return
	 *        An iterator which iterates over all the field TDItems
	 *        that are included in this TupleDesc
	 * */
	public Iterator<TDItem> iterator() {
		// some code goes here
		return null;
	}

	private static final long serialVersionUID = 1L;
	private List <TDItem> fieldList;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 * @param fieldAr
	 *            array specifying the names of the fields. Note that names may
	 *            be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		// TODO: some code goes here
		int len = typeAr.length;
		fieldList = new ArrayList<>();
		for(int i=0; i<len; i++){
			fieldList.add(new TDItem(typeAr[i], fieldAr[i]));
		}

	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 *
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		// TODO: some code goes here
		int len = typeAr.length;
		fieldList = new ArrayList<>();
		for(int i=0; i<len; i++){
			fieldList.add(new TDItem(typeAr[i], null));
		}
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// TODO: some code goes here
		return fieldList.size();
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i
	 *            index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		// TODO: some code goes here, don't forget to check index range!
		// + return "null" string for null case

		int len = this.numFields();
		if(i >= 0 && i < len){

			String name = fieldList.get(i).fieldName;

			if(name == null){
				return "null";
			}

			return name;

		}

		else{
			throw new NoSuchElementException();
		}


	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i
	 *            The index of the field to get the type of. It must be a valid
	 *            index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		// TODO: some code goes here

		int len = this.numFields();
		if(i >= 0 && i < len){

			return fieldList.get(i).fieldType;

		}

		else{
			throw new NoSuchElementException();
		}
	}

	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name
	 *            name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException
	 *             if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		// TODO: some code goes here
		// hint! how to throw exception? refer hashCode() function in this class

		int len = this.numFields();
		for(int i=0; i<len; i++) {
			String field = fieldList.get(i).fieldName;
			if(field != null && field.equals(name)) {
				return i;
			}
		}
		
		throw new NoSuchElementException();
		
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		// TODO: some code goes here

		int size = 0;

		int len = this.numFields();
		for(int i=0; i<len; i++){
			Type t = fieldList.get(i).fieldType;
			if(t == Type.INT_TYPE)
				size += Type.INT_TYPE.getLen();
			else
				size += Type.STRING_TYPE.getLen();
	
		}

		return size;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 *
	 * @param td1
	 *            The TupleDesc with the first fields of the new TupleDesc
	 * @param td2
	 *            The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		// TODO: some code goes here

    int len1 = td1.numFields();
    int len2 = td2.numFields();

		Type[] type = new Type[len1+len2];
		String[] name = new String[len1+len2];


		for(int i=0; i<len1; i++){
			type[i] = td1.getFieldType(i);
			name[i] = td1.getFieldName(i);
		}

		for(int i=0; i<len2; i++){
			type[len1+i] = td2.getFieldType(i);
			name[len1+i] = td2.getFieldName(i);
		}

		return new TupleDesc(type, name);
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 *
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {

		if ( o == null || ! o.getClass().equals(this.getClass()) )
			return false;

		// TODO: some code goes here

		int len = this.numFields();
		TupleDesc td = (TupleDesc) o;

		if(len != td.numFields()){
			return false;
		}

		for(int i=0; i<len; i++){
			if( this.getFieldType(i) != td.getFieldType(i)){
				return false;
			}
		}

		return true;



	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 *
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// some code goes here
		return "";
	}
}
