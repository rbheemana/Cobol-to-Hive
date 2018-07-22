package com.savy3.mapred;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;


/**
 * MainframeVBInputFormat is an input format used to read input files which
 * contain binary data in which record starts with RDW (record length) of 4 bytes. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MainframeVBInputFormat extends
		FileInputFormat<LongWritable, BytesWritable> {

	private long minSplitSize = 1;
	private Seekable filePosition;
	private long splitCount=0;
	private long totalRecords=0;
	private long totalSize = 0; // compute total size
	@Override
	public RecordReader<LongWritable, BytesWritable> getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		reporter.setStatus(genericSplit.toString());
		return new MainframeVBRecordReader(job, (FileSplit) genericSplit);
	}

	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return true;
	}

	/**
	 * Splits files returned by {@link #listStatus(JobConf)} when they're too
	 * big.
	 */
	@Override
	@SuppressWarnings("deprecation")
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {

		FileStatus[] files = listStatus(job);		
		for (FileStatus file : files) { // check we have valid files
			if (file.isDir()) {
				throw new IOException("Not a file: " + file.getPath());
			}
			totalSize += file.getLen();
		}

		long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
		long minSize = Math.max(job.getLong("mapred.min.split.size", 1),
				minSplitSize);
		// generate splits
		ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job);
			FSDataInputStream fileIn;
			InputStream inputStream;
			fileIn = fs.open(path);
			inputStream = fileIn;
			filePosition = fileIn;
			long offset = 0;
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					length);
			if ((length != 0) && isSplitable(fs, path)) {
				long blockSize = file.getBlockSize();

				long bytesRemaining = length;
				long splitSize =0;
				while (offset < length) {
					splitSize = computeSplitSize(goalSize, minSize,
							blockSize, inputStream);
					
					
					int blkIndex = getBlockIndex(blkLocations, length
							- bytesRemaining);
					splits.add(new FileSplit(path, length - bytesRemaining,
							splitSize, blkLocations[blkIndex].getHosts()));
					
					bytesRemaining -= splitSize;
					offset = length-bytesRemaining;
				}

				if (bytesRemaining != 0) {
					throw new IOException("Partial record(length = "
					+ bytesRemaining + ") found at the end of file "+path);
				}
			} else if (length != 0) {
				splits.add(new FileSplit(path, 0, length, blkLocations[0]
						.getHosts()));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
			if (inputStream != null) {
				inputStream.close();
				inputStream = null;
			}
		}
		java.util.Date date= new java.util.Date();
		System.out.println((new Timestamp(date.getTime()))+",\t Split = 100%  Total Splits - "+(++splitCount)+"\t Total Records in VB file - "+totalRecords);

		LOG.debug("Total # of splits: " + splits.size());
		return splits.toArray(new FileSplit[splits.size()]);
	}

	public static int getRDW(byte[] b) {
		int val = 0;
		for (int i = 0; i < 2; i++) {
			int low = b[i] & 0x0F;
			int high = (b[i] >> 4) & 0x0f;
			if (low < 0)
				low *= -1;
			if (high < 0)
				high *= -1;
			int num = high * 16 + low;
			val = 256 * val + num;
		}

		return val;
	}

	protected long computeSplitSize(long goalSize, long minSize,
			long blockSize, InputStream inputStream) throws IOException {
		byte[] lengthByte = new byte[4];
		
		int numBytesRead = 0,numRecordsRead =0;
		long splitSize = 0;
		long numBytesRemainingInSplit = Math.max(minSize, Math.min(goalSize, blockSize));
		java.util.Date date= new java.util.Date();
		while (numBytesRemainingInSplit > 0) {
			int numBytesToRead = 4;
			while (numBytesToRead > 0) {
				byte[] tempByte = new byte[4];
				numBytesRead = inputStream.read(tempByte, 0, numBytesToRead);
				if (numBytesRead == -1) {
					// EOF
					int percentCompletion = (int)(filePosition.getPos()*100/totalSize);
					System.out.println((new Timestamp(date.getTime()))+", Split = "+percentCompletion+"%,\t Split No: "+(++splitCount)+"\t start Pos: "+(filePosition.getPos()-splitSize)+"\t splitsize: " + splitSize+"\t Records in split: "+numRecordsRead);
					totalRecords+=numRecordsRead;
					return splitSize;
				}
				for (int i = 0; i < numBytesRead; i++) {
					lengthByte[4 - numBytesToRead + i] = tempByte[i];
				}
				numBytesToRead -= numBytesRead;
			}
			if (numBytesToRead == 0) {
				int currentRecordLength = getRDW(lengthByte);
				splitSize += currentRecordLength;
				numBytesRemainingInSplit -= currentRecordLength;
				numRecordsRead++;
				inputStream.skip(currentRecordLength - 4);
			} else {
				System.out.println("Error reading RDW byte");
			}

		}
		
		int percentCompletion = (int)(filePosition.getPos()*100/totalSize);
		System.out.println((new Timestamp(date.getTime()))+", Split = "+percentCompletion+"%,\t Split No: "+(++splitCount)+"\t start Pos: "+(filePosition.getPos()-splitSize)+"\t splitsize: " + splitSize+"\t Records in split: "+numRecordsRead);
		totalRecords+=numRecordsRead;
		return splitSize;
	}

}
