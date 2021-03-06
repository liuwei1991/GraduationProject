package common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/*
 * github: https://github.com/liuwei1991/GraduationProject.git
 */
public class DataGenT2 {
	int num = 0;
	int keyLen = 8;
	int columnNum = 4;
	String filePath = "";

	public DataGenT2(int num, int keyLen, int columnNum, String filePath) {
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

		for (int i = 0; i < this.num; i++) {
			String rowKey = "";
			for (int j = 0; j < keyLen; j++) {
				rowKey += String.valueOf(r.nextInt(10));
			}
			for (int j = 0; j < this.columnNum; j++) {
				rowKey += " " + r.nextInt(10000);
			}
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
			if (curNum == fileNum - 1) {
				numPerFile = numPerFile + this.num % fileNum;
			}
			for (int i = 0; i < numPerFile; i++) {
				String rowKey = "";
				for (int j = 0; j < keyLen; j++) {
					rowKey += String.valueOf(r.nextInt(10));
				}
				for (int j = 0; j < this.columnNum; j++) {
					rowKey += " " + r.nextInt(10000);
				}
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
		int keylen[] = {8,16,24,32};
		int threadNum = 8;
		int columnNum = 4;

		for (int len : keylen) {
			for (int i = 1; i <= 5; i++) {
				DataGenT2 dg = new DataGenT2(1000 * i * base, len, columnNum,
						"/ares/TestData/t2/thread=" + threadNum + "/keylen="
								+ len + " columnNum=" + columnNum + "/" + 1000
								* i + "w");
				if (threadNum <= 1) {
					dg.genData();
				} else {
					dg.genData(threadNum);
				}
			}
		}
	}
}
