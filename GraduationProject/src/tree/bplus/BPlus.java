package tree.bplus;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import common.Value;

public class BPlus {

	private final ConcurrentNavigableMap<String, Value> delegatee;

	public void test() {
		
	}
	
	public BPlus(final Comparator c) {
		this.delegatee = new ConcurrentSkipListMap<String, Value>(c);
	}

	public BPlus(final ConcurrentNavigableMap<String, Value> m) {
		this.delegatee = m;
	}

	public Value first() {
		return this.delegatee.get(this.delegatee.firstKey());
	}

	public Value last() {
		return this.delegatee.get(this.delegatee.lastKey());
	}

	public boolean add(String key,String column,String value) {
		if(this.delegatee.containsKey(key)){
			this.delegatee.get(key).putValue(column,value);
			return true;
		}else{
			return this.delegatee.put(key, new Value(column, value)) == null;
		}
	}
	
	public boolean add(String key,Map<String,String> kvs) {
		if(this.delegatee.containsKey(key)){
			this.delegatee.get(key).putValue(kvs);
			return true;
		}else{
			return this.delegatee.put(key, new Value(kvs)) == null;
		}
	}
	
	public String get(String key,String column) {
		return this.delegatee.get(key).getValue(column);
	}
	
	public boolean remove(Object o) {
		return this.delegatee.remove(o) != null;
	}
	
	public ConcurrentSkipListMap<String, Value> scan(String fromKey,boolean fromInclusive,String toKey,boolean toInclusive){
		return (ConcurrentSkipListMap<String, Value>) this.delegatee.subMap(fromKey, fromInclusive, toKey, toInclusive);
	}

	public void testScan(){
		for(Entry<String,Value> e: this.delegatee.entrySet()){
			String key = e.getKey();
			for(Entry<String,String> kv:e.getValue().getAllKV().entrySet()){
				String column = kv.getKey();
				String value = kv.getValue();
			}
		}
	}
	
	public void clear() {
		this.delegatee.clear();
	}

	public boolean contains(Object o) {
		// noinspection SuspiciousMethodCalls
		return this.delegatee.containsKey(o);
	}

	public boolean isEmpty() {
		return this.delegatee.isEmpty();
	}



	public int size() {
		return this.delegatee.size();
	}
	
	public Iterator<Value> descendingIterator() {
		return this.delegatee.descendingMap().values().iterator();
	}
	
	public Iterator<Value> iterator() {
		return this.delegatee.values().iterator();
	}
	
	/**
	 * public SortedSet<String> headSet(final String toElement) { return
	 * headSet(toElement, false); }
	 * 
	 * public NavigableSet<String> headSet(final String toElement, boolean
	 * inclusive) { return new
	 * StringSkipListSet(this.delegatee.headMap(toElement, inclusive)); }
	 */


	/**
	 * public SortedSet<String> tailSet(String fromElement) { return
	 * tailSet(fromElement, true); }
	 * 
	 * public NavigableSet<String> tailSet(String fromElement, boolean
	 * inclusive) { return new
	 * StringSkipListSet(this.delegatee.tailMap(fromElement, inclusive)); }
	 */


	
	
	

	public Object[] toArray() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public String lower(String e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public String pollFirst() {
		throw new UnsupportedOperationException("Not implemented");
	}
	public String higher(String e) {
		throw new UnsupportedOperationException("Not implemented");
	}
	public String ceiling(String e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public NavigableSet<String> descendingSet() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public String floor(String e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public String pollLast() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public SortedSet<String> subSet(String fromElement, String toElement) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public NavigableSet<String> subSet(String fromElement,
			boolean fromInclusive, String toElement, boolean toInclusive) {
		throw new UnsupportedOperationException("Not implemented");
	}
	
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}
	
	public boolean addAll(Collection<? extends String> c) {
		throw new UnsupportedOperationException("Not implemented");
	}
	
	public Comparator<? super String> comparator() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

}
