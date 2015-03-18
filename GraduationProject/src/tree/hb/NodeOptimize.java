package tree.hb;

import java.util.concurrent.ConcurrentSkipListMap;

import common.Value;

public class NodeOptimize {
	private ConcurrentSkipListMap<String,NodeOptimize> nextLayer;
	private boolean isLeaf;
	private boolean isValue;
	private Value value;
	
	public NodeOptimize(){
		this.nextLayer = null;
		this.isValue = false;
		this.value = null;
	}
	
	public NodeOptimize(boolean isValue,boolean isLeaf,Value value, ConcurrentSkipListMap<String,NodeOptimize> nextLayer){
		this.nextLayer = nextLayer;
		this.isValue = isValue;
		this.value = value;
		this.isLeaf = isLeaf;
	}

	public ConcurrentSkipListMap<String, NodeOptimize> getNextLayer() {
		return nextLayer;
	}

	public void setNextLayer(ConcurrentSkipListMap<String, NodeOptimize> nextLayer) {
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

	public boolean isLeaf() {
		return isLeaf;
	}

	public void setLeaf(boolean isLeaf) {
		this.isLeaf = isLeaf;
	}
	
}
