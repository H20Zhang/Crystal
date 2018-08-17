package sPreprocess.sTool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ReaderTest extends Configured implements Tool {

    private static Logger log = Logger.getLogger(ReaderTest.class);

    public static class ReaderTestMapper extends Mapper<Object, Object, NullWritable, NullWritable> {
	@Override
	protected void map(Object key, Object value, Context context)
		throws IOException, InterruptedException {
	   int a = 1;
	   a++;
	}
    }

    @Override
    public int run(String[] args) throws Exception {

	Configuration conf = getConf();

	int memory_size = conf.getInt("test.memory", 4000);
	String memory_opts = "-Xmx" + memory_size + "M";

	conf.setInt("mapreduce.map.memory.mb", memory_size);
	conf.set("mapreduce.map.java.opts", memory_opts);
	conf.setInt("mapreduce.reduce.memory.mb", memory_size);
	conf.set("mapreduce.reduce.java.opts", memory_opts);

	Job job = new Job(conf);

	job.setJarByClass(getClass());
	job.setJobName("Preprocess");

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(NullOutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));

	job.setNumReduceTasks(0);

	job.setMapperClass(ReaderTestMapper.class);

	
	boolean success = job.waitForCompletion(true);
	
	

	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	
//	conf.setNumMapTasks(12);
//	conf.setNumReduceTasks(12);
//	conf.set("mapreduce.local.map.tasks.maximum", "12");
//	conf.set("mapreduce.local.reduce.tasks.maximum", "12");
//	conf.set("mapred.job.tracker", "local");
//	conf.set("mapreduce.framework.name", "local");
	
	ToolRunner.run(conf, new ReaderTest(), args);
	endTime = System.currentTimeMillis();
	log.info("[reader test] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
