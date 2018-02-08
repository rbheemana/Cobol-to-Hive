package com.savy3.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.sql.Timestamp;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sqoop.mapreduce.mainframe.MainframeConfiguration;

//import org.apache.sqoop.util.MainframeFTPClientUtils;

public class FTPtoHDFS

{
	private static FTPClient ftp = null;

	public static void main(String[] args) throws IOException,
			URISyntaxException {
		JobConf job = new JobConf(FTPtoHDFS.class);
		job.set("mapreduce.jdbc.url",args[0]);
		job.set("mapreduce.jdbc.username",args[1]);
		job.set("mapreduce.jdbc.password",args[2]);
		job.set("mapreduce.mainframe.input.dataset.name", args[3]);
		
		DBConfiguration.configureDB(job, "manu", job.get(DBConfiguration.URL_PROPERTY),
				job.get(DBConfiguration.USERNAME_PROPERTY), job.get(DBConfiguration.PASSWORD_PROPERTY));
		ftp = MainframeFTPClientUtils.getFTPConnection(job);
		ftp.setFileType(FTP.BINARY_FILE_TYPE);
		ftp.featureValue("LITERAL SITE RDW");
		ftp.doCommand("SITE", "RDW");
		System.out.println("reply for LITERAL" + ftp.getReplyString());
		String dsName = "'"
				+ job.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME)
				+ "'";
		ftp.changeWorkingDirectory(dsName);
		InputStream inputStream = ftp.retrieveFileStream(dsName);

		FileSystem fileSystem = FileSystem.get(job);
		java.util.Date date = new java.util.Date();
		OutputStream outputStream = fileSystem.create(new Path(args[4]));
		System.out.println("FTP Start Time : "
				+ (new Timestamp(date.getTime())));
		IOUtils.copyBytes(inputStream, outputStream, job, true);
		date = new java.util.Date();
		System.out.println("FTP End Time : " + (new Timestamp(date.getTime())));

	}

}
