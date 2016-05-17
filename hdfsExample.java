package com.fishpan1209;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.BasicConfigurator;

import java.io.*;

public class hdfsExample {

	public static void main(String[] args) throws IOException {

		FileSystem hdfs = FileSystem.get(new Configuration());
		BasicConfigurator.configure();

		// Print the home directory
		System.out.println("Home folder -" + hdfs.getHomeDirectory());

		// Create & Delete Directories
		Path workingDir = hdfs.getWorkingDirectory();
		Path newFolderPath = new Path("/Output");
		newFolderPath = Path.mergePaths(workingDir, newFolderPath);

		if (hdfs.exists(newFolderPath)) {
			// Delete existing Directory
			hdfs.delete(newFolderPath, true);
			System.out.println("Existing Folder Deleted.");
		}

		// Create new Directory
		hdfs.mkdirs(newFolderPath);
		System.out.println("Folder Created at: "+hdfs.getWorkingDirectory());

		// Copying File from local to HDFS
		Path localFilePath = new Path("/users/aojing/dropbox/liaison/project/data/test/");
		Path hdfsFilePath = new Path(newFolderPath + "/test");

		hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
		System.out.println("File copied from local to HDFS.");
		
		/*

		// Copying File from HDFS to local
		localFilePath = new Path("/users/aojing/dropbox/liaison/project/output/test.txt");
		hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		System.out.println("Files copied from HDFS to local.");

		// Creating a file in HDFS
		Path newFilePath = new Path(newFolderPath + "/newFile.txt");
		hdfs.createNewFile(newFilePath);
         */
		
		// Writing data to a HDFS file
		StringBuilder sb = new StringBuilder();
		for (int i = 1; i <= 5; i++) {
			sb.append("Data");
			sb.append(i);
			sb.append("\n");
		}

		byte[] byt = sb.toString().getBytes();
		Path newFilePath = new Path(newFolderPath+"/newFile.txt");
		FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
		fsOutStream.write(byt);
		fsOutStream.close();
		System.out.println("Written data to HDFS file.");

		// Reading data From HDFS File
		System.out.println("Reading from HDFS file.");
		BufferedReader bfr = new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
		String str = null;
		while ((str = bfr.readLine()) != null) {
			System.out.println(str);
		}
	}
}
