package tree.bplus;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class BSkipListTest {

	private final ConcurrentNavigableMap<String, Value> delegatee;

	public void test() {

	}

	BSkipListTest(final Comparator c) {
		this.delegatee = new ConcurrentSkipListMap<String, Value>(c);
	}

	BSkipListTest(final ConcurrentNavigableMap<String, Value> m) {
		this.delegatee = m;
	}

	public String ceiling(String e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public Iterator<Value> descendingIterator() {
		return this.delegatee.descendingMap().values().iterator();
	}

	public NavigableSet<String> descendingSet() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public String floor(String e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * public SortedSet<String> headSet(final String toElement) { return
	 * headSet(toElement, false); }
	 * 
	 * public NavigableSet<String> headSet(final String toElement, boolean
	 * inclusive) { return new
	 * StringSkipListSet(this.delegatee.headMap(toElement, inclusive)); }
	 */

	public String higher(String e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public Iterator<Value> iterator() {
		return this.delegatee.values().iterator();
	}

	public String lower(String e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public String pollFirst() {
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

	/**
	 * public SortedSet<String> tailSet(String fromElement) { return
	 * tailSet(fromElement, true); }
	 * 
	 * public NavigableSet<String> tailSet(String fromElement, boolean
	 * inclusive) { return new
	 * StringSkipListSet(this.delegatee.tailMap(fromElement, inclusive)); }
	 */

	public Comparator<? super String> comparator() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public Value first() {
		return this.delegatee.get(this.delegatee.firstKey());
	}

	public Value last() {
		return this.delegatee.get(this.delegatee.lastKey());
	}

	public boolean add(String e, Value v) {
		return this.delegatee.put(e, v) == null;
	}

	public boolean addAll(Collection<? extends String> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public void clear() {
		this.delegatee.clear();
	}

	public boolean contains(Object o) {
		// noinspection SuspiciousMethodCalls
		return this.delegatee.containsKey(o);
	}

	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public boolean isEmpty() {
		return this.delegatee.isEmpty();
	}

	public boolean remove(Object o) {
		return this.delegatee.remove(o) != null;
	}

	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public Value get(String kv) {
		return this.delegatee.get(kv);
	}

	public int size() {
		return this.delegatee.size();
	}

	public Object[] toArray() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException("Not implemented");
	}

}
