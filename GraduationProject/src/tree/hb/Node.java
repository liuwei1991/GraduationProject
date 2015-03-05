package tree.hb;

import java.util.concurrent.ConcurrentSkipListMap;

import tree.bplus.Value;

public class Node {
	private ConcurrentSkipListMap<String,Node> nextLayer;
	private boolean isValue;
	private Value value;
	
	public Node(){
		this.nextLayer = null;
		this.isValue = false;
		this.value = null;
	}

	public ConcurrentSkipListMap<String, Node> getNextLayer() {
		return nextLayer;
	}

	public void setNextLayer(ConcurrentSkipListMap<String, Node> nextLayer) {
		this.nextLayer = nextLayer;
	}

	public boolean isValue() {
		return isValue;
	}

	public void setIsValue(boolean isValue) {
		this.isValue = isValue;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}
	
}
