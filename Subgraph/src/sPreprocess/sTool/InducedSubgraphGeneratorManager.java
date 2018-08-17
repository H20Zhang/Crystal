package sPreprocess.sTool;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class InducedSubgraphGeneratorManager extends Configured {

    public static void WriteFile(String hdfs, String message) throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(hdfs), conf);
	FSDataOutputStream hdfsOutStream = fs.create(new Path(hdfs));
	hdfsOutStream.writeChars(message);
	hdfsOutStream.close();
	fs.close();
    }

    public static void main(String[] args) throws IOException {
	Configuration conf = new Configuration();
	conf.set("percent", args[3]);

	/*
	 * conf.set("mapred.job.tracker", "local"); conf.set("fs.defaultFS",
	 * "file:///");
	 */

	// conf.set("mapreduce.reduce.memory.mb", "4096");
	// conf.set("mapreduce.reduce.java.opts", "-Xmx4096M");
	// Run map1 and reduce1
	long l3 = System.currentTimeMillis();
	int exitCode = 0;
	try {
	    exitCode = ToolRunner.run(conf, new InducedSubgraphGeneratorA(), args);
	    exitCode = ToolRunner.run(conf, new InducedSubgraphGeneratorB(), args);
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	l3 = System.currentTimeMillis() - l3;
	WriteFile(args[2] + "/time", Long.toString(l3));

	System.exit(exitCode);
    }

}
