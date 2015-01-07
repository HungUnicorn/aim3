package de.tuberlin.dima.aim3.assignment4;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;

/*Combine the file in one folder into one file */ 
public class CombineFile {
	final static String FILE_READ = Config.pathToUpload();
	final static String FILE_WRITE = Config.pathToCombinedUpload();
	static ArrayList<String> StringList = new ArrayList<String>();

	public static void main(String[] Args) throws IOException {
		Read();
		write();
	}

	public static void Read() throws IOException {
		BufferedReader bufferedReader = null;
		File folder = new File(FILE_READ);
		File[] listOfFiles = folder.listFiles();		
		for (File file : listOfFiles) {
			if (file.isFile()) {
				bufferedReader = new BufferedReader(new FileReader(file));
				String s = "";
				while ((s = bufferedReader.readLine()) != null) {					
					StringList.add(s);
				}
			}
		}
		IOUtils.closeQuietly(bufferedReader);
	}

	public static void write() throws IOException {
		BufferedWriter bufferedWriter = null;
		bufferedWriter = new BufferedWriter(new FileWriter(FILE_WRITE));
		for (String r : StringList) {
			bufferedWriter.write(r);
			bufferedWriter.newLine();
			bufferedWriter.flush();
		}
		IOUtils.closeQuietly(bufferedWriter);
	}
}
