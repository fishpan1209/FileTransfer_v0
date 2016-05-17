package com.fishpan1209;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.tools.DistCp; 
/*
Transfer5:
directory creation: 
header copying: 
file copying:  file transfer within hdfs
 */
public class Transfer5 {
	
	public Transfer5(){
		
	}
	
	public void getInputData(FileSystem hdfs, String localSrcPath, String inputPath) throws IOException{
		
		

		// Print the home directory
		System.out.println("Home folder -" + hdfs.getHomeDirectory());

		// Create Working Directories, if already exists, delete it and create new one
		Path workingDir = hdfs.getWorkingDirectory();
		Path newFolderPath = new Path(inputPath);
		newFolderPath = Path.mergePaths(workingDir, newFolderPath);
		
		// if Input path doesn't exist, create it
		if(!hdfs.exists(newFolderPath)){
			hdfs.mkdirs(newFolderPath);
			System.out.println("Folder created at: "+newFolderPath);
		}

		// Copying File from local to HDFS
		
		String FolderName = localSrcPath.substring(localSrcPath.lastIndexOf('/')+1,localSrcPath.length());
		
		Path localFilePath = new Path(localSrcPath);
		Path hdfsFilePath = new Path(newFolderPath + "/"+FolderName);
		
		// if directory already exists, delete it
		if(hdfs.exists(hdfsFilePath)){
			hdfs.delete(hdfsFilePath,true);
			System.out.println("Existing "+hdfsFilePath.toString()+" has been deleted");
		}

		hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
		System.out.println("File copied from local to HDFS.");
		
	}
	
	public void transferDir(FileSystem hdfs, String hdfsSrc, String hdfsDst) throws Exception{
		System.out.println("Copying directory from "+hdfsSrc+" to "+hdfsDst);
		Path workingDir = hdfs.getWorkingDirectory();
		Path destPath = new Path(hdfsDst);
		destPath = Path.mergePaths(workingDir, destPath);
		
		// if Output path doesn't exist, create it
		if(!hdfs.exists(destPath)){
			hdfs.mkdirs(destPath);
			System.out.println("Folder created at: "+destPath.toString());
		}
		
		Configuration conf = hdfs.getConf();
		DistCp distcp = new DistCp(conf, null);
		ToolRunner.run(distcp, new String[]{hdfsSrc, hdfsDst});
		
	}
	
	public void sendFile(){
		
	}

	public static void main(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		// TODO Auto-generated method stub
		String localFilePath = "/users/aojing/dropbox/Liaison/Project/Data/corpus";
		String inputPath = "/Input";
		String hdfsSrc = "/Input";
		String hdfsDst = "/Output";
		Transfer5 t5 = new Transfer5();
		t5.getInputData(hdfs, localFilePath, inputPath);
		long startTime = System.currentTimeMillis();
		t5.transferDir(hdfs, hdfsSrc, hdfsDst);
		long endTime = System.currentTimeMillis();
        System.out.println("Total Time is  " + (endTime - startTime)+"ms");

	}

}
