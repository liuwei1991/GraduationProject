package tree.hb;

import java.util.concurrent.ConcurrentSkipListMap;

import tree.bplus.Value;

public class Data {
	private ConcurrentSkipListMap<String,Data> nextLayer;
	private boolean isValue;
	private Value value;
	
	public Data(){
		this.nextLayer = new ConcurrentSkipListMap<String,Data>();
		this.isValue = false;
		this.value = null;
	}

	public ConcurrentSkipListMap<String, Data> getNextLayer() {
		return nextLayer;
	}

	public void setNextLayer(ConcurrentSkipListMap<String, Data> nextLayer) {
		this.nextLayer = nextLayer;
	}

	public boolean isValue() {
		return isValue;
	}

	public void setValue(boolean isValue) {
		this.isValue = isValue;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}
	
}
