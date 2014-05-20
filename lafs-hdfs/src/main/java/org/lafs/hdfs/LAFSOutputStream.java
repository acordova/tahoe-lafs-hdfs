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

/**
 *  The reason for this class is to call conn.getResponseCode()
 *  upon close()
 *  
 */

package org.lafs.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Syncable;



public class LAFSOutputStream extends OutputStream  implements Syncable {

	private static final Logger logger = Logger.getLogger(LAFSOutputStream.class.getName());
	
	private HttpURLConnection conn;
	private OutputStream stream;


	public LAFSOutputStream(HttpURLConnection conn) throws IOException { //, LAFS lafs, Path path) throws IOException {
		this.stream = conn.getOutputStream();
		System.out.println(this.stream.getClass().getCanonicalName());
		this.conn = conn;
	}
	
	@Override
	public void write(int b) throws IOException {
		stream.write(b);
	}

	@Override
	public void close() throws IOException {
		stream.close();

		int code = conn.getResponseCode();
		logger.log(Level.INFO, "closed stream with code {0}", code);
	}

	@Override
	public void sync() throws IOException {
		logger.log(Level.INFO, "sync called");
	}

	@Override
	public void hflush() throws IOException {
		logger.log(Level.INFO, "hflush called");
	}

	@Override
	public void hsync() throws IOException {
		logger.log(Level.INFO, "hsync called");
	}	
}
