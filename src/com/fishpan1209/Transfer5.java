package com.fishpan1209;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
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
	
	public void transfer(FileSystem hdfs, String hdfsSrc, String hdfsDst) throws Exception{
		System.out.println("Copying directory from "+hdfsSrc+" to "+hdfsDst);
		Path workingDir = hdfs.getWorkingDirectory();
		Path destPath = new Path(hdfsDst);
		destPath = Path.mergePaths(workingDir, destPath);
		Path srcPath = new Path(hdfsSrc);
		srcPath = Path.mergePaths(workingDir, srcPath);
		
		// if Output path doesn't exist, create it
		if(!hdfs.exists(destPath)){
			hdfs.mkdirs(destPath);
			System.out.println("Folder created at: "+destPath.toString());
		}
		
		transferDir(srcPath, destPath,hdfs);
	}
	
	public void transferDir(Path srcDir, Path destDir, FileSystem hdfs) throws FileNotFoundException, IOException {
		// start copying directory
		if (!hdfs.exists(srcDir)) {
			System.out.println("Directory " + srcDir.toString() + " does not exist.");
			System.exit(0);
		} else {
			if (hdfs.isDirectory(srcDir)) {
				// if destination directory not exists, create it
				if (!hdfs.exists(destDir)) {
					hdfs.mkdirs(destDir);
					System.out.println("Directory "+destDir.toString()+" has been created");
				}

				// list all the directory contents
				FileStatus[] fileStatus = hdfs.listStatus(srcDir);

				for (FileStatus file : fileStatus) {
					// construct the src and dest file structure
					if (file.isDirectory()) {
						transferDir(file.getPath(), destDir, hdfs);
					} else {
						// copyfile
						LocalFileSystem localFS = hdfs.getLocal(hdfs.getConf());
						File localFile = localFS.pathToFile(srcDir);
						System.out.println("Sending file: "+localFile.getName());
						ProcessFile processer = new ProcessFile(localFile.toPath());
						String header = processer.getHeader();
						sendFile(srcDir, destDir, header,hdfs);
					}
				}
			}
		}
	}
	
	public void sendFile(Path hdfsSrc, Path hdfsDst, String header,FileSystem hdfs) throws IOException{
		FileUtil.copy(hdfs, hdfsSrc, new File(hdfsDst.toString()), false,hdfs.getConf());
	}

	public static void main(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		// TODO Auto-generated method stub
		String localFilePath = "/users/aojing/dropbox/Liaison/Project/Data/test";
		String inputPath = "/Input";
		String hdfsSrc = "/Input/test";
		String hdfsDst = "/Output";
		Transfer5 t5 = new Transfer5();
		//t5.getInputData(hdfs, localFilePath, inputPath);
		long startTime = System.currentTimeMillis();
		t5.transfer(hdfs, hdfsSrc, hdfsDst);
		long endTime = System.currentTimeMillis();
        System.out.println("Total Time of Transfer5 is  " + (endTime - startTime)+"ms");

	}

}
