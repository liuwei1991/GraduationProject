package common;

import java.util.HashMap;
import java.util.Map;

public class Value {
	private Map<String, String> keyValue = new HashMap<String, String>();

	public Value(String column,String value){
		this.keyValue.put(column, value);
	}
	
	public Value(Map<String,String> kvs) {
		this.keyValue.putAll(kvs);
	}
	
	public String getValue(String column){
		return this.keyValue.get(column);
	}

	public void putValue(String column,String value){
		this.keyValue.put(column, value);
	}
	
	public void putValue(Map<String,String> kvs){
		this.keyValue.putAll(kvs);
	}
	
	public Map<String, String> getAllKV(){
		return this.keyValue;
	}
	
	/*
	public Value(String value) {
		this.value = value;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
   */
}
