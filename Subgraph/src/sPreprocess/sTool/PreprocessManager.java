package sPreprocess.sTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

public class PreprocessManager extends Configured {

    public static void main(String[] args) {
	Configuration conf = new Configuration();

	// conf.set("mapred.job.tracker", "local"); conf.set("fs.defaultFS",
	// "file:///");

	conf.set("mapreduce.map.memory.mb", "5000");
	conf.set("mapreduce.map.java.opts", "-Xmx1500M");
	conf.set("mapreduce.reduce.memory.mb", "5000");
	conf.set("mapreduce.reduce.java.opts", "-Xmx1000M");

	// Run map1 and reduce1
	int exitCode = 0;
	try {
	    exitCode = ToolRunner.run(conf, new Preprocess(), args);
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	System.exit(exitCode);
    }
}
