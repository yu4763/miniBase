package minibase;

import java.io.IOException;


/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private int tableId;
    private DbIterator child;
    private DbIterator[] children;
    private TransactionId tid;
    private int recordscnt;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // TODO: some code goes here
    	
    	tid = t;
    	tableId = tableid;
    	this.child = child;
    	recordscnt = 0;
    	
    	children = new DbIterator[1];
    	children[0] = child;
    	
    	
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
    	
    	Catalog c = Database.getCatalog();
    	HeapFile file = (HeapFile) c.getDbFile(tableId);
    
        return file.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO some code goes here
	// hint: you have to consider parent class as well
    	
    	recordscnt = 0;
    	child.open();
    	
    }

    public void close() {
        // TODO: some code goes here
    	
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
    	
    	recordscnt = 0;
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
	// hint: insert Tuples passed by iterator (child) to Buffer.
    	

    	if(recordscnt > 0)
    		return null;
    	
		BufferPool b = Database.getBufferPool();
		
		while(child.hasNext()) {
			Tuple item = child.next();
			if(!item.getTupleDesc().equals(getTupleDesc()))
				throw new DbException("");
			
			try {
				b.insertTuple(tid, tableId, item);
				recordscnt++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		Type []type = new Type[1];
		type[0] = Type.INT_TYPE;
		TupleDesc td = new TupleDesc(type);
		
		Tuple tuple = new Tuple(td);
		
		tuple.setField(0, new IntField(recordscnt));
		
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // TODO: some code goes here
	// hint! there is only one element you can pass through DbIterator[]
    	return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // TODO: some code goes here
    	this.children = children;
    	
    }
}
