package tree.bplus;

public class Value {
	private long ts;
	private String column;
	private String value;
	
	public Value(String column,long ts,String value){
		this.ts = ts;
		this.column = column;
		this.value = value;
	}
	
	public Value(String column,String value){
		this.column = column;
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
	
}
