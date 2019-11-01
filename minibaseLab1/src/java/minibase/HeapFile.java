package minibase;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see minibase.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	File file;
	TupleDesc td;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f the file that stores the on-disk backing store for this heap file.
	 */

	public HeapFile(File f, TupleDesc td) {
		// TODO: some code goes here
		this.file = f;
		this.td = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		// TODO: some code goes here
		return file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note: you
	 * will need to generate this tableid somewhere ensure that each HeapFile has a
	 * "unique id," and that you always return the same value for a particular
	 * HeapFile. We suggest hashing the absolute file name of the file underlying
	 * the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		// TODO: some code goes here
		return file.getAbsoluteFile().hashCode();

	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		// TODO: some code goes here
		return td;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		// TODO: some code goes here
		// hint!! to read specific page at arbitrary offset you need random access to
		// the file

		RandomAccessFile rf;
		Page p = null;

		try {

			rf = new RandomAccessFile(file.getAbsolutePath(), "r");
			long position = (long) BufferPool.PAGE_SIZE * pid.pageNumber();

			if (rf.length() < position + BufferPool.PAGE_SIZE) {
				rf.close();
				throw new IllegalArgumentException();
			}

			rf.seek(position);
			byte[] bytes = new byte[BufferPool.PAGE_SIZE];
			rf.read(bytes);
			rf.close();

			HeapPageId hpid = (HeapPageId) pid;
			p = new HeapPage(hpid, bytes);

		} catch (IOException e) {

			e.printStackTrace();
		}

		return p;

	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// TODO: some code goes here
		
		RandomAccessFile rf;
		long position = (long) BufferPool.PAGE_SIZE * page.getId().pageNumber();
		

		rf = new RandomAccessFile(file.getAbsolutePath(), "rw");
		rf.seek(position);
		
		rf.write(page.getPageData());

		rf.close();
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// TODO: some code goes here
		// hint!! you can calculate number of pages as you know PAGE_SIZE
		int num = (int) (file.length() / BufferPool.PAGE_SIZE);
		return num;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// TODO: some code goes here

		ArrayList<Page> pages = new ArrayList<Page>();
		boolean inserted = false;

		int n = numPages();
		for (int i = 0; i < n; i++) {
			HeapPageId pid = new HeapPageId(getId(), i);
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			if (page.getNumEmptySlots() != 0) {
				page.insertTuple(t);
				pages.add(page);
				inserted = true;
				break;
			}
		}
		
		if(!inserted) {
			HeapPageId pid = new HeapPageId(getId(), n);
			byte [] bytes = new byte[BufferPool.PAGE_SIZE];
			HeapPage page = new HeapPage(pid, bytes);
			writePage(page);
			page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			page.insertTuple(t);
			pages.add(page);
		}
			
			
		return pages;

	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// TODO: some code goes here

		RecordId rid = t.getRecordId();

		if (rid == null)
			throw new DbException("");

		PageId pid = rid.getPageId();

		if (pid == null | pid.getTableId() != getId())
			throw new DbException("");

		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
		page.deleteTuple(t);
		return page;

	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// TODO: some code goes here
		return new HeapFileIterator(tid, getId(), numPages());
	}

	// TODO: make HeapFileIterator class, you can freely add new methods, variable
	/**
	 * Class for iterating over all tuples of this file
	 *
	 * @see minibase.DbFileIterator
	 */
	private class HeapFileIterator implements DbFileIterator {

		TransactionId tid;
		int tableId;
		int numPages;
		HeapPage page;
		Iterator<Tuple> iter;
		int count = 1;
		boolean open = false;

		/**
		 * Constructor for iterator
		 *
		 * @param tid      Transactional of requesting transaction
		 * @param tableId  of the HeapFile
		 * @param numPages the number of pages in file
		 */
		public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
			// hint: you can get tuple iterator from HeapPage
			this.tid = tid;
			this.tableId = tableId;
			this.numPages = numPages;

			List<Tuple> t = new ArrayList<Tuple>();
			iter = t.iterator();

		}

		/**
		 * Open it iterator for iteration
		 *
		 * @throws DbException
		 * @throws TransactionAbortedException
		 */
		public void open() throws DbException, TransactionAbortedException {

			page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, 0), Permissions.READ_ONLY);
			iter = page.iterator();
			open = true;

		}

		/**
		 * Check if iterator has next tuple
		 *
		 * @return boolean true if exists
		 * @throws DbException
		 * @throws TransactionAbortedException
		 */
		public boolean hasNext() throws DbException, TransactionAbortedException {

			if (!open)
				return false;

			if (iter.hasNext())
				return true;

			while (count < numPages) {
				count++;
				page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, count - 1),
						Permissions.READ_ONLY);
				iter = page.iterator();
				if(iter.hasNext()) {
					return true;
				}
					
			}

		
			return false;
		}

		/**
		 * Get next tuple in this file
		 *
		 * @return
		 * @throws DbException
		 * @throws TrnasactionAbortedException
		 * @throws NoSuchElementException
		 */
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

			if (!hasNext())
				throw new NoSuchElementException();

			return iter.next();
		}

		/**
		 * Rewind iterator to the start of file
		 *
		 * @throws DbException
		 * @throws TransactionAbortedException
		 */
		public void rewind() throws DbException, TransactionAbortedException {
			count = 1;
			page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, count - 1),
					Permissions.READ_ONLY);
			iter = page.iterator();
		}

		/**
		 * Close the iterator
		 */
		public void close() {

			page = null;
			List<Tuple> t = new ArrayList<Tuple>();
			iter = t.iterator();
			open = false;

		}
	}

}
