package minibase;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;



/**
 * LockManager is class which manages locks for transactions. It stores states
 * of various locks on pages and provides atomic grant and release of locks.
 * 
 * @author hrishi
 */
public class LockManager {

	private Hashtable<PageId, pageLock> lockTable;
	private Hashtable<TransactionId, Set<PageId>> pidTable; 
	private Hashtable<PageId, SharedLock> waitTableSh;
	private Hashtable<PageId, ArrayList<ExclusiveLock>> waitTableEx;


	class MyThread implements Runnable {
		@Override
		public void run() { //작업할 내용 }
			block();
		}
	
	}

	public LockManager() {
		lockTable = new Hashtable<PageId, pageLock>();
		pidTable = new Hashtable<TransactionId, Set<PageId>>();
		waitTableSh = new Hashtable<PageId, SharedLock>();
		waitTableEx = new Hashtable<PageId, ArrayList<ExclusiveLock>>();
	}

	/**
	 * Checks if transaction has lock on a page
	 * 
	 * @param tid Transaction Id
	 * @param pid Page Id
	 * @return boolean True if holds lock
	 */
	public boolean holdsLock(TransactionId tid, PageId pid) {

		if(lockTable.containsKey(pid)) {
			pageLock lock = lockTable.get(pid);
			return lock.IsHeldBy(tid);
		}

		return false;
	}

	/**
	 * Grants lock to the Transaction.
	 * 
	 * @param tid  TransactionId requesting lock.
	 * @param pid  PageId on which the lock is requested.
	 * @param perm The type of permission.
	 */
	public void requestLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
			
		
		if(holdsLock(tid, pid)) {
			
			pageLock lock = lockTable.get(pid);
			if(lock instanceof ExclusiveLock) {
				return;
			}
			
			else {
				
				SharedLock sl = (SharedLock) lock;
				if(perm == Permissions.READ_ONLY) {
					return;
				}
				else {
					
					lock.unlock(tid);
					if(sl.lockCounts() == 0) {
						lock = new ExclusiveLock(tid);
					}
					else {
						
						sl.state = STATE.WAITING;
						waitTableEx.computeIfAbsent(pid, k -> new ArrayList<ExclusiveLock>());
						
						ExclusiveLock el = new ExclusiveLock(tid);
						waitTableEx.get(pid).add(el);
						
						while(waitTableEx.containsKey(pid) && waitTableEx.get(pid).contains(el)) {
							try {
								Thread.sleep(50);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								Thread.currentThread().interrupt();
							}
						}
						
					}
					
				}
				
				
				
			}
			
			return;
			
		}
		
		pidTable.computeIfAbsent(tid, k-> new HashSet<PageId>());
		Set<PageId> pids = pidTable.get(tid);
		if(!pids.contains(pid)) {
			pids.add(pid);
		}
		
		if(lockTable.containsKey(pid)) {
			
			pageLock lock = lockTable.get(pid);
			
			if(lock instanceof SharedLock) {
				
				SharedLock sl = (SharedLock) lock;
				
				if(sl.state == STATE.SHARED) {  // PRESENT STATE: SHARED
					
					
					if(perm == Permissions.READ_ONLY) {	
						sl.lock(tid);
							
						
					}
					
					else {
						
						
						sl.state = STATE.WAITING;
						waitTableEx.computeIfAbsent(pid, k -> new ArrayList<ExclusiveLock>());
						
						ExclusiveLock el = new ExclusiveLock(tid);
						waitTableEx.get(pid).add(el);
						
						while(waitTableEx.containsKey(pid) && waitTableEx.get(pid).contains(el)) {
							try {
								Thread.sleep(50);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								Thread.currentThread().interrupt();
							}
						}
							
						
					}
					
					
					
				}
				else {  // PRESENT STATE: WAITING
					
					waitTableSh.computeIfAbsent(pid, k -> new SharedLock());
					
					
					waitTableSh.get(pid).lock(tid);
					
					while(waitTableSh.containsKey(pid) && waitTableSh.get(pid).IsHeldBy(tid)) {
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							Thread.currentThread().interrupt();
						}
					}
					
					
				}
				
				
			}
			
			else {  // PRESENT STATE: EXCLUSIVE
				
				ExclusiveLock el = (ExclusiveLock) lock;
				
				if(perm == Permissions.READ_ONLY) {
					
					waitTableSh.computeIfAbsent(pid, k -> new SharedLock());
					
					waitTableSh.get(pid).lock(tid);
					
					while(waitTableSh.containsKey(pid) && waitTableSh.get(pid).IsHeldBy(tid)) {
						
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							Thread.currentThread().interrupt();
						}
						
					}
					
				}
				else {
					
					waitTableEx.computeIfAbsent(pid, k -> new ArrayList<ExclusiveLock>());
					
					ExclusiveLock nel = new ExclusiveLock(tid);
					waitTableEx.get(pid).add(nel);
					
					while(waitTableEx.containsKey(pid) && waitTableEx.get(pid).contains(nel)) {
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							Thread.currentThread().interrupt();
						}
					}
					
				}
				
				
				
			}
				
			
		}
		else { // PRESENT STATE: UNLOCKED
			
			
			if(perm == Permissions.READ_ONLY) {
				
				lockTable.put(pid, new SharedLock(tid));
			}
			else {
				
				lockTable.put(pid, new ExclusiveLock(tid));
			}
			

			
		}
			

	}
	
	public void block() {
		
		//System.out.println("blocked");
		try {
			
	           Thread.currentThread().wait();
	           
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	

	/**
	 * Releases locks associated with given transaction and page.
	 * 
	 * @param tid The TransactionId.
	 * @param pid The PageId.
	 */
	public synchronized void releaseLock(TransactionId tid, PageId pid) {
	
		
		pageLock lock = lockTable.get(pid);
		lock.unlock(tid);
		
		if(lock instanceof ExclusiveLock) {
			if(waitTableEx.contains(pid)) {
			
				
				lock = waitTableEx.get(pid).get(0);
				waitTableEx.get(pid).remove(0);
				
				
				if(waitTableEx.get(pid).size() == 0 ) {
					waitTableEx.remove(pid);
				}
				
			}
			else if(waitTableSh.contains(pid)) {
				
				lock = waitTableSh.get(pid);
				waitTableSh.remove(pid);
				
			}
			else {
				lockTable.remove(pid);
			}
		}
		else {
			SharedLock sl = (SharedLock) lock;
			if(sl.lockCounts() == 0) {
				if(sl.state == STATE.WAITING) {
					
					if(waitTableEx.contains(pid)) {
						
						lock = waitTableEx.get(pid).get(0);
						waitTableEx.get(pid).remove(0);
						
						if(waitTableEx.get(pid).size() == 0 ) {
							waitTableEx.remove(pid);
						}

					}
					
					
				}
				else {
					lockTable.remove(pid);
				}
			}
		}
		
		
		pidTable.get(tid).remove(pid);

		
	}

	/**
	 * Releases Lock related to a page
	 * 
	 * @param pid PageId
	 */
	public synchronized void removePage(PageId pid) {
		
		pageLock lock = lockTable.get(pid);
		
		if(waitTableEx.contains(pid)) {
			
			lock = waitTableEx.get(pid).get(0);
			waitTableEx.get(pid).remove(0);
			
			if(waitTableEx.get(pid).size() == 0 ) {
				waitTableEx.remove(pid);
			}
			
			notifyAll();
		}
		else if(waitTableSh.contains(pid)) {
			
			lock = waitTableSh.get(pid);
			waitTableSh.remove(pid);
			
			notifyAll();
			
			
		}
		else {
			lockTable.remove(pid);
		}
		
		
	}

	/**
	 * Releases all pages associated with given Transaction.
	 * 
	 * @param tid The TransactionId.
	 */

	public synchronized void releaseAllPages(TransactionId tid) {
		
		HashSet<PageId> temp = (HashSet<PageId>) pidTable.get(tid);
		
		if(temp == null)
			return;
		
	
		HashSet<PageId> pids = new HashSet<PageId>(temp);
		for (PageId p : pids ) {
			
			if(lockTable.containsKey(p)) {
				releaseLock(tid, p);
				
			}
			
		}
	
		
	}
	
	
	public enum STATE implements Serializable {
		
		 UNLOCKED, SHARED, WAITING, EXCLUSIVE;

	}
	
	
	interface pageLock {
		
		
		void lock(TransactionId tid);

		void unlock(TransactionId tid);

		Boolean IsHeldBy(TransactionId tid);
		

	}

	class SharedLock implements pageLock {

		
		Set<TransactionId> tids;
		
		STATE state;

		public SharedLock() {
			tids = new HashSet<TransactionId>();
			this.state = STATE.UNLOCKED;
		}
		
		public SharedLock(TransactionId tid) {
			tids = new HashSet<TransactionId>();
			tids.add(tid);
			this.state = STATE.SHARED;
		}

		@Override
		public void lock(TransactionId tid) {
			tids.add(tid);
			this.state = STATE.SHARED;
		}

		@Override
		public void unlock(TransactionId tid) {

			if (tids.contains(tid)) {
				tids.remove(tid);
			
			} 
		}

		@Override
		public Boolean IsHeldBy(TransactionId tid) {

			return tids.contains(tid);
		}
		
		public int lockCounts() {
			return tids.size();
		}

	}

	class ExclusiveLock implements pageLock {

		TransactionId tid;
		boolean isHeld;
		STATE state;

		public ExclusiveLock() {
			tid = null;
			isHeld = false;
			state = STATE.UNLOCKED;
		}
		
		public ExclusiveLock(TransactionId tid) {
			this.tid = tid;
			this.state = STATE.EXCLUSIVE;
			isHeld = true;
		}

		@Override
		public void lock(TransactionId tid) {
			this.tid = tid;
			isHeld = true;
			state = STATE.EXCLUSIVE;
		}

		@Override
		public void unlock(TransactionId tid) {

			if (this.tid == tid) {
				tid = null;
				isHeld = false;
				state = STATE.UNLOCKED;
			} else {
				throw new IllegalArgumentException();
			}

		}

		@Override
		public Boolean IsHeldBy(TransactionId tid) {

			return this.tid == tid;
		}

		public Boolean canHold() {
			return !isHeld;
		}

	}

}
