package sPreprocess.sTool;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class BloomFilterGeneratorManager extends Configured {

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
	conf.set("bloom_size", args[2]);
	conf.set("bit_per_record", args[3]);
	conf.set("hash_number", args[4]);

	// conf.set("mapred.job.tracker", "local"); conf.set("fs.defaultFS",
	// "file:///");

	conf.set("mapreduce.reduce.memory.mb", "2048");
	conf.set("mapreduce.reduce.java.opts", "-Xmx1024M");
	// Run map1 and reduce1
	long l3 = System.currentTimeMillis();
	int exitCode = 0;
	try {
	    exitCode = ToolRunner.run(conf, new BloomFilterGen(), args);
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	l3 = System.currentTimeMillis() - l3;
	WriteFile(args[1] + "/time", Long.toString(l3));

	System.exit(exitCode);
    }

}
