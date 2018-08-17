package sDeprecated;

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
import sData.IntDegreeArray;
import sData.TwoNodeKey;

import java.io.IOException;

public class TriangleO extends Configured implements Tool {

    private static Logger log = Logger.getLogger(TriangleO.class);

    public static class TriangleOMapper extends Mapper<TwoNodeKey, IntDegreeArray, NullWritable, NullWritable> {
	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    context.getCounter("test", "core").increment(1);
	    context.getCounter("test", "list_size").increment(value.size);
	    context.getCounter("test", "pattern_size").increment(value.size);
	}
    }

    @Override
    public int run(String[] args) throws Exception {

	Configuration conf = getConf();

	int memory_size = conf.getInt("test.memory", 4000);
	int opts_size = (int) (memory_size * 0.85);

	String memory_opts = "-Xmx" + opts_size + "M";

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

	job.setMapperClass(TriangleOMapper.class);

	boolean success = job.waitForCompletion(true);

	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	ToolRunner.run(conf, new TriangleO(), args);
	endTime = System.currentTimeMillis();
	log.info("[Triangle] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
