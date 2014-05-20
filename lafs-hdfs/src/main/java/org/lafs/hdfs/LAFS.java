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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

public class LAFS extends FileSystem {
	
	private static final Logger logger = Logger.getLogger(LAFS.class.getName());

	private int writeChunkSize = 1024 * 1024 * 1024; // default 100 MB
	
	private String rootCap;
	private String workingDir = "/";
	private URI uri;
	private URI httpURI;

	public String getLAFSPath(Path path) {
		String pathString;
		
		if(path.toString().startsWith("lafs://"))
			pathString = rootCap + path.toUri().getPath().toString();
		else if(path.toString().startsWith("/"))
			pathString = rootCap + path.toString();
		else
			pathString = rootCap + workingDir + path.toString();

		return pathString;
	}

	private JSONArray getJSONForPath(Path path) throws IOException {
		
		URL url = new URL(httpURI.toString() + "/uri/" + getLAFSPath(path) + "?t=json");

		URLConnection uc = url.openConnection();
		uc.setDoInput(true);

		BufferedReader br;
		try {
			br = new BufferedReader(new InputStreamReader(uc.getInputStream()));
		} catch (ConnectException e1) {
			logger.severe(url.toString());
			
			throw new IOException(e1);
		}

		StringBuilder sb = new StringBuilder();

		String line = br.readLine();
		while(line != null) {
			sb.append(line);
			line = br.readLine();
		}

		br.close();

		//logger.info(sb.toString());
		
		JSONArray ja;

		try {
			ja = new JSONArray(new JSONTokener(sb.toString()));
		} catch (JSONException e) {
			throw new IOException(e.getMessage());
		}

		return ja;
	}

	private FileStatus getStatusFromJSON(JSONArray ja, Path path) throws IOException {
		FileStatus stat;

		String flag;
		try {
			flag = ja.getString(0);

			boolean isDir = flag.equals("dirnode");

			JSONObject data =ja.getJSONObject(1);

			long mtime = 0L;
			if(data.has("metadata"))
				mtime = (long)data.getJSONObject("metadata").getJSONObject("tahoe").getDouble("linkmotime");
			
			// each file consists of 1 block of a size the entire length of the file
			
			long size=0L;
			
			if(!isDir) {
				try {
					size = (long)data.getInt("size");
				} catch(JSONException joe) {
					logger.warning("size was null");
				}
			}
			//else 
			//	size = data.getJSONObject("children").length();

			stat = new FileStatus(isDir ? 0 : size, isDir, 1, size, mtime, path);
		} catch (JSONException e) {
			logger.severe(ja.toString());
			throw new IOException(e.getMessage());
		}

		return stat;		
	}

	public LAFS() {

	}


	@Override
	public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) 
	throws IOException {
		
		long offset = 0;
		boolean isDir = false;
		
		// if the file exists, find file size to use as offset in PUT call
		try {
			FileStatus status = getFileStatus(path);
			
			isDir = status.isDirectory();
				
			offset = status.getLen();
		} catch(IOException ioe) {
			// file doesn't exist
		}
		
		if(isDir)
			throw new IOException("Cannot append. Path is a directory");
		
		
		String req = httpURI + "/uri/" + getLAFSPath(path);
		req += "?format=MDMF&offset=" + offset;
		URL url = new URL(req);
		//System.out.println(req);

		// Open the connection and prepare to POST
		HttpURLConnection uc = (HttpURLConnection) url.openConnection();
		uc.setDoOutput(true);
		uc.setRequestMethod("PUT");
		uc.setChunkedStreamingMode(writeChunkSize); 

		
		return new FSDataOutputStream(new LAFSOutputStream(uc)); //, this, path));
	}


	@Override
	public FSDataOutputStream create(Path path) throws IOException {
		return create(path, false);
	}

	@SuppressWarnings("deprecation")
	@Override
	public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
		
		boolean exists = true;
		
		try {
			FileStatus status = getFileStatus(path);
			
		} catch(IOException ioe) {
			// file doesn't exist
			exists = false;
		}
		
		if(exists)
			throw new IOException("File exists.");
		
		// TODO url-encode file names. directory names too?
		// TODO get mutable flag from FsPermission object
		String req = httpURI.toString() + "/uri/" + getLAFSPath(path);
		// create mutable files
		req += "?format=MDMF";
		URL url = new URL(req);
		//System.out.println(req);

		// Open the connection and prepare to POST
		HttpURLConnection uc = (HttpURLConnection) url.openConnection();
		uc.setDoOutput(true);
		uc.setRequestMethod("PUT");
		uc.setChunkedStreamingMode(writeChunkSize); // 200mb chunks 

		return new FSDataOutputStream(new LAFSOutputStream(uc)); //, this, path));
	}

	@Override
	public FSDataOutputStream create(Path path, FsPermission perm,
			boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
	throws IOException {

		return create(path, overwrite);
	}



	@Override
	public boolean delete(Path path) throws IOException {
		return delete(path, false);
	}

	@Override
	public boolean delete(Path path, boolean recursive) throws IOException {
		if(recursive) {
			logger.info("delete called on " + path.toString());
			URL url = new URL(httpURI.toString() + "/uri/" + getLAFSPath(path));
			HttpURLConnection uc = (HttpURLConnection)url.openConnection();

			uc.setDoOutput(true);
			uc.setRequestMethod("DELETE");
			uc.setRequestProperty("Content-Type", "application/x-www-form-urlencoded" );

			uc.connect();
			int code = uc.getResponseCode();
			return code == HttpURLConnection.HTTP_OK;
		}
		else {
			// check to see that this is not a dir
			// if it is a dir, check to see that is it empty
			//then delete
			URL url = new URL(httpURI.toString() + "/uri/" + getLAFSPath(path));
			HttpURLConnection uc = (HttpURLConnection)url.openConnection();

			uc.setDoOutput(true);
			uc.setRequestMethod("DELETE");
			uc.setRequestProperty("Content-Type", "application/x-www-form-urlencoded" );

			uc.connect();
			int code = uc.getResponseCode();
			return code == HttpURLConnection.HTTP_OK;
		}
	}


	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		JSONArray ja = getJSONForPath(path);
		
		return getStatusFromJSON(ja, path);
	}

	@Override
	public URI getUri() {
		return uri;
	}


	@Override
	public Path getWorkingDirectory() {
		return new Path(workingDir);
	}

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf); 

		this.rootCap = URLEncoder.encode(conf.get("lafs.rootcap"), "UTF-8");
		this.uri = uri;  
		this.httpURI = URI.create("http://" + uri.getHost() + ":" + uri.getPort());
		
		this.writeChunkSize = conf.getInt("lafs.write_chunk_size", 1024 * 1024 * 1024);
		
		//if(!rootCap.endsWith("/"))
		//	rootCap = rootCap + "/";
	}

	@SuppressWarnings("unchecked")
	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		FileStatus[] ret = null;

		JSONArray ja = getJSONForPath(path);

		try {
			String flag = ja.getString(0); 
			boolean isDir = flag.equals("dirnode");

			if(isDir) { // process directory
				String pathString = path.toString();
				if(!pathString.endsWith("/"))
					pathString = pathString + "/";

				JSONObject data = ja.getJSONObject(1);

				// get the status of each child
				ArrayList<FileStatus> stats = new ArrayList<>();
				JSONObject  children = data.getJSONObject("children");
				Iterator<String> items = children.keys();
				while(items.hasNext()) {
					String name = items.next();

					FileStatus stat = getStatusFromJSON(children.getJSONArray(name), new Path(pathString + name)); 
					stats.add(stat);
				}

				ret = new FileStatus[stats.size()];
				ret = stats.toArray(ret);
			}
			else { // process a file	
				FileStatus stat = getStatusFromJSON(ja, path);
				ret = new FileStatus[1];
				ret[0] = stat;
			}

		} catch (JSONException e) {
			throw new IOException(e.getMessage());
		}

		return ret;
	}

	@Override
	public boolean mkdirs(Path path, FsPermission perms) throws IOException {
		// TODO make use of perms
		
		if(super.exists(path)) {
			logger.info("mkdir: path exists" + path.toString());
			return true;
		}
		
		int retries = 3;
		for(int i=0; i < retries; i++) {
			URL url = new URL(httpURI.toString() + "/uri/" + getLAFSPath(path.getParent()) + "?t=mkdir&name=" + path.getName());

			// Open the connection and prepare to POST
			HttpURLConnection uc = (HttpURLConnection)url.openConnection();
			uc.setRequestMethod("POST");
			uc.setDoOutput(true);

			uc.connect();

			int code = uc.getResponseCode();
			if(code != HttpURLConnection.HTTP_OK)
				return false;
		
			// verify directory now exists
			try {
				FileStatus stat = getFileStatus(path);
				if(stat != null)
					return true;
			}
			catch (FileNotFoundException fnfe) {
				continue;
			}
		}
		return false;
	}

	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		URL url = new URL(httpURI.toString() + "/uri/" + getLAFSPath(path));

		return  new FSDataInputStream(new LAFSInputStream(url));
	}

	@Override
	public FSDataInputStream open(Path path) throws IOException {
		return open(path, 0);
	}

	public String getWriteCap(Path path) throws IOException {
		return null;
	}

	public String getReadCap(Path path) throws IOException {
		return null;
	}

	@Override
	public boolean rename(Path path, Path newPath) throws IOException {
		
		Path parent = urlEncodePathParts(path.getParent().toUri().getPath());
		String oldName = URLEncoder.encode(path.getName(), "UTF-8");
		Path newParent = urlEncodePathParts(path.getParent().toUri().getPath());
		String newName = URLEncoder.encode(newPath.getName(), "UTF-8");;
		
		URL url = new URL(httpURI.toString() + "/uri/" + getLAFSPath(parent) + "?t=relink&from_name=" + oldName + "&to_dir=" + getLAFSPath(newParent) + "&to_name=" + newName);
		logger.log(Level.INFO, "==== RENAME ==== " + url.toString());
		
		HttpURLConnection uc = (HttpURLConnection)url.openConnection();
		uc.setRequestMethod("POST");
		uc.setDoOutput(true);

		uc.connect();

		int code = uc.getResponseCode();
		if(code != HttpURLConnection.HTTP_OK) {
			logger.log(Level.INFO, "failed to rename: {0}", code);
			logger.log(Level.INFO, "failed to rename: {0}", url.toString());
		}
		
		return code == HttpURLConnection.HTTP_OK;
	}

	@Override
	public void setWorkingDirectory(Path path) {
		workingDir = path.getName();

		if(!workingDir.endsWith("/"))
			workingDir = workingDir + "/";
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		LAFS lafs = new LAFS();
		
		Configuration conf = new Configuration();
		conf.set("lafs.rootcap", "URI:DIR2:wtcbxiktrnvwud56n5lcbsr5pq:lal6wjuco7ewllizjkx6xlobqwtswozyhy2v6bqxwtjdi7rniwqa");
		
		lafs.initialize(new URI("lafs://localhost:3456"), conf);
		
		lafs.mkdirs(new Path("/one/two/three/four"));
	}
	
	public static Path urlEncodePathParts(String pathString) throws UnsupportedEncodingException {
		
		String[] parts = pathString.split("/");
		for(int i=0; i < parts.length; i++)
			parts[i] = URLEncoder.encode(parts[i], "UTF-8");
		return new Path(StringUtils.join(parts, "/"));
	}
}
