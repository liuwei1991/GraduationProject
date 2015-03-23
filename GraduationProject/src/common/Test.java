package common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class Test {
	public static void main(String[] args){
		ConcurrentSkipListMap<String,String> c = new ConcurrentSkipListMap<String,String>();
//		c.put(null, "asdfasd");
		Map<String, String> keyValue = new HashMap<String, String>();
		keyValue.put(null, "sdf");
		System.out.println(keyValue.get(null));
//		System.out.println(c.get(null));
	}
}
