package common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

public class DataGenTPCH {
	int num = 0;
	int keyLen = 8;
	int columnNum = 4;
	String filePath = "";

	public DataGenTPCH(int num, int keyLen, int columnNum, String filePath) {
		this.num = num;
		this.keyLen = keyLen;
		this.filePath = filePath;
		this.columnNum = columnNum;
	}

	public void genData() throws IOException {
		File file = new File(this.filePath + ".txt");

		if (file.exists()) {
			System.out.println("File exists, newly created.");
		}
		file.createNewFile();
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		Random r = new Random();
		int totalNum = 0;
		String[] status = { "done", "undone" };

		for (int i = 0; i < this.num; i++) {
			StringBuilder rowKey = new StringBuilder();

			for (int j = 0; j < keyLen; j++) {
				rowKey.append(r.nextInt(10));
			}
			rowKey.append(" "+r.nextInt(1000000));
			rowKey.append(" "+status[r.nextInt(2)]);
			rowKey.append(" "+r.nextDouble());
			Calendar c = Calendar.getInstance();

			rowKey.append(" "+c.get(Calendar.YEAR) + "-" + c.get(Calendar.MONTH)
					+ "-" + c.get(Calendar.DATE) + "-"
					+ c.get(Calendar.HOUR_OF_DAY) + "-"
					+ c.get(Calendar.MINUTE) + "-" + c.get(Calendar.SECOND));
			
			rowKey.append(" "+r.nextLong());
			rowKey.append(" "+r.nextLong());
			rowKey.append(" "+r.nextInt());
			rowKey.append(" "+r.nextLong());
			bw.write(rowKey + "\r\n");
			totalNum++;
		}
		bw.flush();
		bw.close();
		fw.close();
		System.out.println("Number: " + totalNum);
	}

	public void genData(int fileNum) throws IOException {
		int numPerFile = this.num / fileNum;
		int totalNum = 0;
		File file = new File(this.filePath);
		file.mkdirs();
		for (int curNum = 0; curNum < fileNum; curNum++) {
			file = new File(this.filePath + "/" + curNum + ".txt");

			if (file.exists()) {
				System.out.println("File exists, newly created.");
			}
			file.createNewFile();
			FileWriter fw = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(fw);
			Random r = new Random();
			String[] status = { "done", "undone" };
			
			if (curNum == fileNum - 1) {
				numPerFile = numPerFile + this.num % fileNum;
			}
			for (int i = 0; i < numPerFile; i++) {
				StringBuilder rowKey = new StringBuilder();

				for (int j = 0; j < keyLen; j++) {
					rowKey.append(r.nextInt(10));
				}
				rowKey.append(" "+r.nextInt(1000000));
				rowKey.append(" "+status[r.nextInt(2)]);
				rowKey.append(" "+r.nextDouble());
				Calendar c = Calendar.getInstance();

				rowKey.append(" "+c.get(Calendar.YEAR) + "-" + c.get(Calendar.MONTH)
						+ "-" + c.get(Calendar.DATE) + "-"
						+ c.get(Calendar.HOUR_OF_DAY) + "-"
						+ c.get(Calendar.MINUTE) + "-" + c.get(Calendar.SECOND));
				
				rowKey.append(" "+r.nextInt());
				rowKey.append(" "+r.nextInt());
				rowKey.append(" "+r.nextInt(20));
				rowKey.append(" "+r.nextLong());
				bw.write(rowKey + "\r\n");
				totalNum++;
			}
			bw.flush();
			bw.close();
			fw.close();
			System.out.println("File-" + curNum + " number: " + numPerFile);
		}
		System.out.println("Total number: " + totalNum);
	}

	public static void main(String[] args) throws IOException {
		
		
		int base = 10000;
		int keylen[] = { 24, 32};
		int threadNum[] = {24,32};
		int columnNum = 8;

		for (int len : keylen) {
			for (int i = 4; i <= 5; i++) {
				for (int tn : threadNum) {
					DataGenTPCH dg = new DataGenTPCH(1000 * i * base, len,
							columnNum, "/ares/TestData/tpch/thread=" + tn
									+ "/keylen=" + len + " columnNum="
									+ columnNum + "/" + 1000 * i + "w");
					if (tn <= 1) {
						dg.genData();
					} else {
						dg.genData(tn);
					}
				}
			}
		}
	}
}
