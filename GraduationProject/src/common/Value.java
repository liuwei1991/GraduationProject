package common;

import java.util.HashMap;
import java.util.Map;

public class Value {
	private Map<String, String> keyValue = new HashMap<String, String>();

	public Value(String column, String value) {
		this.keyValue.put(column, value);
	}
	
	public String getValue(String column){
		return this.keyValue.get(column);
	}

	public void setValue(String cloumn,String value){
		this.keyValue.put(cloumn, value);
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
