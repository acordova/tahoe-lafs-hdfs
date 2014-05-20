/**
 * create a root cap within Tahoe and initialize it for MapReduce
 */
package org.lafs.hdfs;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Init {
	public static void main(String[] args) throws IOException, URISyntaxException {
		
		// create a rootcap for all MapReduce stuff
		URL url = new URL("http://localhost:3456/uri?t=mkdir");
		HttpURLConnection uc = (HttpURLConnection) url.openConnection();
		uc.setRequestMethod("PUT");
		uc.connect();
		
		// get the rootcap back
		BufferedReader br = new BufferedReader(new InputStreamReader(uc.getInputStream()));

		StringBuilder sb = new StringBuilder();

		String line = br.readLine();
		while(line != null) {
			sb.append(line);
			line = br.readLine();
		}

		String rootcap = sb.toString(); 
		br.close();
		
		
		Configuration conf = new Configuration();
		conf.set("lafs.rootcap", rootcap);
		
		System.out.println("generated root cap: " + rootcap);
		
		LAFS lafs = new LAFS();
		
		lafs.initialize(new URI("lafs://localhost"), conf);
		
		lafs.mkdirs(new Path("/tmp/hadoop-hadoop/mapred/system"));
	}
}
