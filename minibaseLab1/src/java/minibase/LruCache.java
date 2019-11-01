package minibase;

import java.util.*;

/**
 * @param <K> Type of the Cache Key
 * @param <V> Type of the Cache Elements
 */
public class LruCache<K, V> {

	int capacity;
	Hashtable<K, V> cache;
	ArrayList<K> chain;

	/**
	 * Constructor
	 * 
	 * @param capacity
	 */
	public LruCache(int capacity) {
		// TODO: some code goes here
		this.capacity = capacity;
		cache = new Hashtable<K, V>();
		chain = new ArrayList<K>();
	}

	public boolean isCached(K key) {
		// TODO: some code goes here
		return cache.containsKey(key);
	}

	public V get(K key) {
		// TODO: some code goes here

		chain.remove(key);
		chain.add(key);
		return cache.get(key);
	}

	public boolean put(K key, V value) {
		// TODO: some code goes here
		if(chain.size() >= capacity) {
			return false;
		}
		
		cache.put(key, value);
		if (chain.contains(key)) {
			chain.remove(key);
		}
		chain.add(key);
		return true;
	}

	public V evict() {
		// TODO: some code goes here
		K key = chain.get(0);
		V val = cache.get(key);	
		return val;
	}
	
	public void remove(K key) {
		chain.remove(key);
		cache.remove(key);
	}

	public int size() {
		// TODO: some code goes here
		return capacity;
	}

	public Iterator<V> iterator() {
		// TODO: some code goes here,,, please implement freely! you can remove and make
		// new method
		return cache.values().iterator();
	}

}
