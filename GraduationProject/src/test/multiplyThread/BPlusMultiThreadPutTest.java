package test.multiplyThread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Comparator;

import common.Value;
import tree.bplus.BPlus;

public class BPlusMultiThreadPutTest implements Runnable{
	private BPlus bskl;
	private String inputFilePath;
	private static int totalNum = 0;
	
	public BPlusMultiThreadPutTest(BPlus bskl,String inputFilePath){
		this.bskl = bskl;
		this.inputFilePath = inputFilePath;
	}

	@Override
	public void run() {
		String id = Thread.currentThread().getName();
		File file = new File(this.inputFilePath+id+".txt");
		if(!file.exists()){
			System.out.println("Input file is not found!");
			return;
		}
		try{
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			while(true){
				String str = br.readLine();
				if(str==null){
					break;
				}
				String[] line = str.split(" ");
				this.bskl.add(line[0], new Value(line[1]));
				synchronized(BPlusMultiThreadPutTest.class){
					totalNum++;
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	
	static long time = System.currentTimeMillis();
	static long startTime = time;
	static long lastNum = 0;
	
	public static Thread output =  new Thread(){
		public void run(){
			while(true){
				try {
					sleep(5000);
					long current = totalNum;
			        System.out.println("Start Time:" + time + "\tNow:"
			            + System.currentTimeMillis() + "\tputNum :" + current
			            + "\tCurrent Speed:" + ((current - lastNum) * 1000)
			            / (System.currentTimeMillis() - time) + "\tTotal Speed:"
			            + (current * 1000) / (System.currentTimeMillis() - startTime)
			            + " r/s");
			        time = System.currentTimeMillis();
			        lastNum = current;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	};
	static{
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
	}
	
	
	public static void main(String[] args){
		Comparator<String> c = new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				if (o1.compareTo(o2) < 0) {
					return -1;
				} else if (o1.compareTo(o2) == 0) {
					return 0;
				}
				return 1;
			}
		};
		
		BPlus bp = new BPlus(c);
		String inputFilePath = "D:/TestData/t2/keylen=16/";
		BPlusMultiThreadPutTest btp = new BPlusMultiThreadPutTest(bp,inputFilePath);
		for(int i=0;i<10;i++){
			Thread t =  new Thread(btp);
			t.setName(String.valueOf(i));
			t.start();
		}
	}
}
