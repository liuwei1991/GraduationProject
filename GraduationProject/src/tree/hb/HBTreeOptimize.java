package tree.hb;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import common.Value;

public class HBTreeOptimize {

	public NodeOptimize rootNodeOpt;
	public int chunkSize = 8;
	private int minLayerNum = 8;
	private Comparator c;

	public HBTreeOptimize(final Comparator c, final int chunkSize,final int minLayerNum) {
		this.c = c;
		this.chunkSize = chunkSize;
		rootNodeOpt = new NodeOptimize(false,true,null,new ConcurrentSkipListMap<String, NodeOptimize>(c));
		this.minLayerNum = minLayerNum;
	}

	public String get(String key, String column) {
		int len = 0;
		ConcurrentNavigableMap<String, NodeOptimize> nodeMap = rootNodeOpt
				.getNextLayer();
		NodeOptimize nodeOpt = rootNodeOpt;
		while (len < key.length()) {
			if(nodeOpt==null){
				return null;
			}
			nodeMap = nodeOpt.getNextLayer();
			if (nodeMap == null) {
				return null;
			}
			if (nodeOpt.isLeaf()) {
				len += chunkSize;
				break;
			} else {
				String curChunk = key.substring(len,
						Math.min(len + chunkSize, key.length()));
				nodeOpt = nodeMap.get(curChunk);
				len += chunkSize;
			}
		}
		NodeOptimize result = nodeMap.get(key.substring(len - chunkSize));
		if (result == null || !result.isValue()) {
			return null;
		} else {
			return result.getValue().getValue(column);
		}
	}

	public boolean add(String key, String column, String value) {
		int len = 0;
		ConcurrentNavigableMap<String, NodeOptimize> nodeMap = rootNodeOpt.getNextLayer();
		NodeOptimize nodeOpt = this.rootNodeOpt;
		while (len < key.length()) {
			nodeMap = nodeOpt.getNextLayer();
			if (nodeOpt.isLeaf()) {
				String cur = key.substring(len);
				this.insertDataNode(nodeOpt, cur, column,value);
				if(nodeOpt.getNextLayer().size()>this.minLayerNum){
					this.adjustLeafNode(nodeOpt);
				}
				return true;
			} else {
				String cur = key.substring(len,	Math.min(len + chunkSize, key.length()));
				nodeOpt = nodeMap.get(cur);
				if (nodeOpt == null) {
					if (len + chunkSize >= key.length()) {
						nodeOpt = new NodeOptimize(true,false,new Value(column,value),
								new ConcurrentSkipListMap<String, NodeOptimize>(c));
						nodeMap.put(cur, nodeOpt);
					} else {
						nodeOpt = new NodeOptimize(false,true,null,
								new ConcurrentSkipListMap<String, NodeOptimize>(c));
						nodeMap.put(cur, nodeOpt);
						this.insertDataNode(nodeOpt, key.substring(len + chunkSize), column,value);
					}
					return true;
				}
				len += chunkSize;
			}
		}
		if (nodeOpt.isValue()) {
			nodeOpt.getValue().putValue(column,value);
		} else {
			nodeOpt.setIsValue(true);
			nodeOpt.setValue(new Value(column,value));
		}
		return true;
	}
	
	public boolean add(String key, Map<String, String> kvs) throws InterruptedException {
		int len = 0;
		ConcurrentNavigableMap<String, NodeOptimize> nodeMap = rootNodeOpt.getNextLayer();
		NodeOptimize nodeOpt = this.rootNodeOpt;
		while (len < key.length()) {
			nodeMap = nodeOpt.getNextLayer();
			if (nodeOpt.isLeaf()) {
				String cur = key.substring(len);
				if(nodeOpt.getNextLayer().size()>this.minLayerNum && !nodeOpt.isAdjust){
					if(!nodeOpt.isAdjust){
						nodeOpt.isAdjust = true;
						this.insertDataNode(nodeOpt, cur, kvs);
						this.adjustLeafNode(nodeOpt);
						nodeOpt.isAdjust = false;
					}else{
						while(nodeOpt.isAdjust){
							Thread.sleep(500);
							this.insertDataNode(nodeOpt, cur, kvs);
						}
					}
				}
				return true;
			} else {
				String cur = key.substring(len,	Math.min(len + chunkSize, key.length()));
				nodeOpt = nodeMap.get(cur);
				if (nodeOpt == null) {
					if (len + chunkSize >= key.length()) {
						nodeOpt = new NodeOptimize(true,false,new Value(kvs),
								new ConcurrentSkipListMap<String, NodeOptimize>(c));
						nodeMap.put(cur, nodeOpt);
					} else {
						nodeOpt = new NodeOptimize(false,true,null,
								new ConcurrentSkipListMap<String, NodeOptimize>(c));
						nodeMap.put(cur, nodeOpt);
						this.insertDataNode(nodeOpt, key.substring(len + chunkSize), kvs);
					}
					return true;
				}
				len += chunkSize;
			}
		}
		if (nodeOpt.isValue()) {
			nodeOpt.getValue().putValue(kvs);
		} else {
			nodeOpt.setIsValue(true);
			nodeOpt.setValue(new Value(kvs));
		}
		return true;
	}
	
	public void adjustLeafNode(NodeOptimize no){
		ConcurrentNavigableMap<String, NodeOptimize> oldMap = no.getNextLayer();
		no.setNextLayer(new ConcurrentSkipListMap<String, NodeOptimize>(c));
		no.setLeaf(false);
		ConcurrentNavigableMap<String, NodeOptimize> newMap = no.getNextLayer();
		for(Entry<String,NodeOptimize> e:oldMap.entrySet()){
			String str = e.getKey();
			NodeOptimize oldNode = e.getValue();
			if(str.length()<=chunkSize){
				this.insertDataNode(no, str, oldNode);
			}else{
				String curChunk = str.substring(0,chunkSize);
				String rest = str.substring(chunkSize);
				NodeOptimize tmp = newMap.get(curChunk);
				if(tmp==null){
					tmp = new NodeOptimize(false,true,null,new ConcurrentSkipListMap<String, NodeOptimize>()); 
					newMap.put(curChunk, tmp);
				}
				this.insertDataNode(tmp, rest, oldNode);
			}
		}
	}
	
	public void insertDataNode(NodeOptimize parent,String key,Map<String, String> kvs){
		NodeOptimize tmp = parent.getNextLayer().get(key);
		if(tmp==null){
			tmp = new NodeOptimize(true,false,new Value(kvs),
					new ConcurrentSkipListMap<String, NodeOptimize>()); 
			parent.getNextLayer().put(key, tmp);
		}else{
			if(tmp.isValue()){
				tmp.getValue().putValue(kvs);
			}else{
				tmp.setIsValue(true);
				tmp.setValue(new Value(kvs));
			}
		}
	}
	
	public void insertDataNode(NodeOptimize parent,String key,String column, String value){
		NodeOptimize tmp = parent.getNextLayer().get(key);
		if(tmp==null){
			tmp = new NodeOptimize(true,false,new Value(column,value),
					new ConcurrentSkipListMap<String, NodeOptimize>()); 
			parent.getNextLayer().put(key, tmp);
		}else{
			if(tmp.isValue()){
				tmp.getValue().putValue(column,value);
			}else{
				tmp.setIsValue(true);
				tmp.setValue(new Value(column,value));
			}
		}
	}

	public void insertDataNode(NodeOptimize parent,String key,NodeOptimize child){
		NodeOptimize tmp = parent.getNextLayer().get(key);
		if(tmp==null){
			parent.getNextLayer().put(key, child);
		}else{
			if(tmp.isValue()){
				tmp.getValue().putValue(child.getValue().getAllKV());
			}else{
				tmp.setIsValue(true);
				tmp.setValue(child.getValue());
			}
		}
	}

	public boolean delete(String key) {
		int len = 0;
		ConcurrentNavigableMap<String, NodeOptimize> nodeMap = rootNodeOpt
				.getNextLayer();
		while (len < key.length()) {
			String curChunk = key.substring(len, len + chunkSize);
			NodeOptimize nodeOpt = nodeMap.get(curChunk);
			if (nodeOpt == null) {
				return false;
			}
			nodeMap = nodeOpt.getNextLayer();
			len += chunkSize;
		}
		NodeOptimize result = nodeMap.get(key.substring(len - chunkSize, len));
		if (result.isValue()) {
			result.setIsValue(false);
		}
		return true;
	}

	public ConcurrentSkipListMap<String, Value> scan(String fromKey,
			String toKey) {
		ConcurrentSkipListMap<String, Value> result = new ConcurrentSkipListMap<String, Value>(
				c);
		this.scan(result, fromKey, toKey, this.rootNodeOpt, 0, "");
		return result;
	}

	public void scan(ConcurrentSkipListMap<String, Value> result,
			String fromKey, String toKey, NodeOptimize nodeOpt, int layer,
			String prefix) {
		if (nodeOpt == null) {
			return;
		}
		String fromKeyLayer = fromKey.substring(layer * chunkSize, (layer + 1)
				* chunkSize);
		Entry<String, NodeOptimize> fromEntry = nodeOpt.getNextLayer()
				.ceilingEntry(fromKeyLayer);
		String toKeyLayer = toKey.substring(layer * chunkSize, (layer + 1)
				* chunkSize);
		Entry<String, NodeOptimize> toEntry = nodeOpt.getNextLayer()
				.floorEntry(toKeyLayer);
		if (fromEntry == null || toEntry == null) {
			return;
		}
		// first node
		if (fromEntry.getValue().isValue()) {
			result.put(prefix, fromEntry.getValue().getValue());
		}
		if (!fromEntry.getKey().equals(fromKeyLayer)) {
			scanAll(result, fromEntry.getValue(), prefix + fromEntry.getKey());
		} else {
			scan(result, fromKey, toKey, fromEntry.getValue(), layer + 1,
					prefix + fromKeyLayer);
		}
		// middle nodes
		ConcurrentSkipListMap<String, NodeOptimize> children = (ConcurrentSkipListMap<String, NodeOptimize>) nodeOpt
				.getNextLayer().subMap(fromKeyLayer, false, toKey, false);
		for (Entry<String, NodeOptimize> entry : children.entrySet()) {
			scanAll(result, entry.getValue(), prefix + entry.getKey());
		}
		// last node
		if (toEntry.getValue().isValue()) {
			result.put(prefix, toEntry.getValue().getValue());
		}
		if (!toEntry.getKey().equals(toKeyLayer)) {
			scanAll(result, toEntry.getValue(), prefix + toKeyLayer);
		} else {
			scan(result, fromKey, toKey, toEntry.getValue(), layer + 1, prefix
					+ toKeyLayer);
		}
	}

	/**
	 * Get all the result under this node.
	 * 
	 * @param result
	 * @param node
	 * @param prefix
	 */
	private void scanAll(ConcurrentSkipListMap<String, Value> result,
			NodeOptimize nodeOpt, String prefix) {
		if (nodeOpt.isValue()) {
			result.put(prefix, nodeOpt.getValue());
		}
		ConcurrentSkipListMap<String, NodeOptimize> children = nodeOpt
				.getNextLayer();
		if (children == null) {
			return;
		}
		for (Entry<String, NodeOptimize> entry : children.entrySet()) {
			scanAll(result, entry.getValue(), prefix + entry.getKey());
		}
	}
}
