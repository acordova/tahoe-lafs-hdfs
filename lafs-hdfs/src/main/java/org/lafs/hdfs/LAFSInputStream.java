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

package org.lafs.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class LAFSInputStream extends InputStream implements Seekable, PositionedReadable {
	
	private long pos;
	private InputStream stream;
	private URL url;
	private boolean closed = true;
	
	public LAFSInputStream(URL url) throws IOException {
		this.url = url;
	}
	
	private void open() throws IOException {
		if(!closed) {
			try {
				stream.close();
			} catch(IOException ioe) {
				ioe.printStackTrace();
			}
		}
		
		closed = false;
		URLConnection uc = url.openConnection();
		uc.setDoInput(true);
		
		stream = uc.getInputStream();
		pos = 0L;
	}
	
	@Override
	public long getPos() throws IOException {
		return pos;
	}

	/**
	 * seek over instance stream
	 */
	@Override
	public void seek(long target) throws IOException {
		
		if(closed)
			open();
		
		// could seek backwards by opening a new stream ...
		if(target < pos) {
			//throw new IOException("can't seek backwards");
			open();
		}
		
		while(pos < target) {
			stream.read();
			pos++;
		}
	}

	@Override
	public boolean seekToNewSource(long offset) throws IOException {
		return false;
	}

	/**
	 * does read over a given section of a file
	 * starts a new stream, doesn't alter 'pos'
	 * is thread safe
	 */
	@Override
	public int read(long position, byte[] buf, int offset, int len)
			throws IOException {
		
		if(closed)
			open();
	
		seek(offset);
		
		int bytesRead;
		for(bytesRead=0; bytesRead < len; bytesRead++)
			buf[offset+bytesRead] = (byte)stream.read();
		
		return bytesRead;
	}

	/**
	 * this doesn't update pos
	 */
	@Override
	public void readFully(long position, byte[] buf) throws IOException {
		read(position, buf, 0, buf.length);
	}

	public void readFully(byte[] buf) throws IOException {
		readFully(0, buf);
	}
	
	@Override
	public void readFully(long position, byte[] buf, int offset, int len)
			throws IOException {
		read(position, buf, offset, len);
	}

	/**
	 * reads from the instance stream
	 * alters pos
	 * isn't thread safe
	 */
	@Override
	public int read() throws IOException {
		if(closed)
			open();
		
		pos++;
		return stream.read();
	}
	
	@Override
	public int available() throws IOException {
		if(closed)
			open();
		
		return stream.available();
	}
	
	@Override
	public void close() throws IOException {
		if(!closed)
			stream.close();
	}
}
