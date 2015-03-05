package tree.hb;

import java.util.Comparator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import tree.bplus.Value;


public class HBTree {
	private final ConcurrentNavigableMap<String, Value> delegatee;
	
	public HBTree(final Comparator c){
		this.delegatee = new ConcurrentSkipListMap<String, Value>(c);
	}
	
	public Value get(String key){
		return this.delegatee.get(key);
	}
	
	public boolean add(String key,Value value){
		this.delegatee.put(key, value);
		return true;
	}
	
	public boolean delete(String key){
		return true;
	}
	
	public ConcurrentSkipListMap<String, Value> scan(String fromKey,boolean fromInclusive,String toKey,boolean toInclusive){
		return (ConcurrentSkipListMap<String, Value>) this.delegatee.subMap(fromKey, fromInclusive, toKey, toInclusive);
	}
	
}
