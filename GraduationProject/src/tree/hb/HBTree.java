package tree.hb;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import tree.bplus.Value;

public class HBTree {
	private final ConcurrentSkipListMap<String, Node> delegatee;
	private int chunkSize = 8;
	private int minLayerNum = 0;
	private Comparator c;

	public HBTree(final Comparator c) {
		this.c = c;
		this.delegatee = new ConcurrentSkipListMap<String, Node>(c);
	}

	public Value get(String key) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = delegatee;
		while (len < key.length()) {
			String curChunk = key.substring(len, len + chunkSize);
			nodeMap = nodeMap.get(curChunk).getNextLayer();
			if (nodeMap == null) {
				return null;
			}
			len += chunkSize;
		}
		Node result = nodeMap.get(key.substring(len - chunkSize, len));
		if (result == null) {
			return null;
		} else {
			return result.getValue();
		}
	}

	public boolean add(String key, Value value) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = delegatee;
		while (len < key.length()) {
			String curChunk = key.substring(len, len + chunkSize);
			Node node = nodeMap.get(curChunk);
			if (node == null) {
				node = new Node();
				node.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
				nodeMap.put(curChunk, node);
			}
			nodeMap = node.getNextLayer();
			len += chunkSize;
		}
		nodeMap.get(key.substring(len - chunkSize, len)).setIsValue(true);
		nodeMap.get(key.substring(len - chunkSize, len)).setValue(value);
		return true;
	}

	public boolean delete(String key) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = delegatee;
		while (len < key.length()) {
			String curChunk = key.substring(len, len + chunkSize);
			Node node = nodeMap.get(curChunk);
			if (node == null) {
				return false;
			}
			nodeMap = node.getNextLayer();
			len += chunkSize;
		}
		Node result = nodeMap.get(key.substring(len - chunkSize, len));
		if (result.isValue()) {
			result.setIsValue(false);
		}
		return true;
	}

	public ConcurrentSkipListMap<String, Node> scan(String fromKey,
			boolean fromInclusive, String toKey, boolean toInclusive) {
		ConcurrentSkipListMap<String, Value> result = new ConcurrentSkipListMap<String, Value>(
				c);
		ConcurrentSkipListMap<String, Node> cur = (ConcurrentSkipListMap<String, Node>) this.delegatee
				.subMap(fromKey, fromInclusive, toKey, toInclusive);

		for (Entry<String, Node> entry : cur.entrySet()) {
			if (entry.getValue().isValue()) {
				result.put(prefix + entry.getKey(), entry.getValue().getValue());
			}
			result.putAll(scan());
		}
		return (ConcurrentSkipListMap<String, Node>) this.delegatee.subMap(
				fromKey, fromInclusive, toKey, toInclusive);
	}

	private void scan(String fromKey, boolean fromInclusive, String toKey,
			boolean toInclusive, String prefix,
			ConcurrentSkipListMap<String, Value> result,Node node) {
		if(node.isValue()){
			result.put(prefix, node.getValue());
		}
		if(node.getNextLayer()==null){
			return;
		}
		ConcurrentSkipListMap<String, Node> cur = (ConcurrentSkipListMap<String, Node>) node.getNextLayer().subMap(fromKey, fromInclusive, toKey, toInclusive);
		
	}

}
