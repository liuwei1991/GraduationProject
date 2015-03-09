package tree.hb;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import common.Value;

public class HBTree {
	private Node rootNode;
	private int chunkSize = 8;
	private int minLayerNum = 0;
	private Comparator c;

	public void test(){
//		this.delegatee.ceilingEntry(key)
	}
	
	public HBTree(final Comparator c) {
		this.c = c;
		rootNode = new Node();
		rootNode.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
	}

	public Value get(String key) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = rootNode.getNextLayer();
		while (len < key.length()) {
			String curChunk = key.substring(len, Math.min(len + chunkSize,key.length()));
			Node node = nodeMap.get(curChunk);
			if(node == null){
				return null;
			}
			nodeMap = node.getNextLayer();
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
		ConcurrentNavigableMap<String, Node> nodeMap = rootNode.getNextLayer();
		Node node = this.rootNode;
		while (len < key.length()) {
			String curChunk = key.substring(len, Math.min(key.length(),len + chunkSize));
			node = nodeMap.get(curChunk);
			if (node == null) {
				node = new Node();
				node.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
				nodeMap.put(curChunk, node);
			}
			nodeMap = node.getNextLayer();
			len += chunkSize;
		}
		node.setIsValue(true);
		node.setValue(value);
		return true;
	}

	public static void printHBTree(Node node){
		Queue<Node> queue = new LinkedList<Node>();
		queue.add(node);
		while(!queue.isEmpty()){
			
		}
	}
	
	public boolean delete(String key) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = rootNode.getNextLayer();
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
	
	public ConcurrentSkipListMap<String, Value> scan(String fromKey, String toKey) {
		ConcurrentSkipListMap<String, Value> result = new ConcurrentSkipListMap<String, Value>(c);
		this.scan(result, fromKey, toKey, this.rootNode, 0, "");
		return result;
	}

	public void scan(ConcurrentSkipListMap<String, Value> result,String fromKey, String toKey,Node node,int layer,String prefix){
		if(node==null){
			return;
		}
		String fromKeyLayer = fromKey.substring(layer*chunkSize, (layer+1)*chunkSize);
		Entry<String,Node> fromEntry = node.getNextLayer().ceilingEntry(fromKeyLayer);
		String toKeyLayer = toKey.substring(layer*chunkSize, (layer+1)*chunkSize);
		Entry<String,Node> toEntry = node.getNextLayer().floorEntry(toKeyLayer);
		if(fromEntry==null || toEntry==null){
			return;
		}
		//first node
		if(fromEntry.getValue().isValue()){
			result.put(prefix, fromEntry.getValue().getValue());
		}
		if(!fromEntry.getKey().equals(fromKeyLayer)){
			scanAll(result,fromEntry.getValue(),prefix+fromEntry.getKey());
		}else{
			scan(result,fromKey, toKey, fromEntry.getValue(), layer+1, prefix+fromKeyLayer);
		}
		//middle nodes
		ConcurrentSkipListMap<String, Node> children = (ConcurrentSkipListMap<String, Node>) node.getNextLayer().subMap(fromKeyLayer, false, toKey, false);
		for(Entry<String,Node> entry:children.entrySet()){
			scanAll(result,entry.getValue(),prefix+entry.getKey());
		}
		//last node
		if(toEntry.getValue().isValue()){
			result.put(prefix, toEntry.getValue().getValue());
		}
		if(!toEntry.getKey().equals(toKeyLayer)){
			scanAll(result, toEntry.getValue(), prefix+toKeyLayer);
		}else{
			scan(result,fromKey, toKey, toEntry.getValue(), layer+1, prefix+toKeyLayer);
		}
	}
	
	/**
	 * Get all the result under this node.
	 * @param result
	 * @param node
	 * @param prefix
	 */
	private void scanAll(ConcurrentSkipListMap<String, Value> result,Node node,String prefix) {
		if(node.isValue()){
			result.put(prefix, node.getValue());
		}
		ConcurrentSkipListMap<String, Node> children = node.getNextLayer();
		if(children==null){
			return;
		}
		for(Entry<String,Node> entry: children.entrySet()){
			scanAll(result,entry.getValue(),prefix+entry.getKey());
		}
	}
	
}
