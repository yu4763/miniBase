package minibase;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private DbIterator[] children;
    private int recordscnt;
    private boolean complete = false;
    

    // hint: implementation of Delete.java is not that much different from implementing Insert.java.
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // TODO: some code goes here
    	this.tid = t;
    	this.child = child;
    	this.recordscnt = 0;
    	
    	children = new DbIterator[1];
    	children[0] = child;
    	
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here   
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
    	super.open();
    	child.open();
    	complete = false;
    }

    public void close() {
        // TODO: some code goes here
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
    	child.rewind();
    	complete = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
    	
    	if(complete)
    		return null;
    	
    	BufferPool b = Database.getBufferPool();
		
		while(child.hasNext()) {
			Tuple item = child.next();
			b.deleteTuple(tid, item);
			recordscnt++;
			
		}
			
		Type []type = new Type[1];
		type[0] = Type.INT_TYPE;
		TupleDesc td = new TupleDesc(type);
		
		Tuple tuple = new Tuple(td);
		
		tuple.setField(0, new IntField(recordscnt));
		
		complete = true;
		
        return tuple;
		
    }

    @Override
    public DbIterator[] getChildren() {
        // TODO: some code goes here
    	return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // TODO: some code goes here
    	this.children = children;
    }

}
