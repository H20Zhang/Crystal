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
import sData.OneNodeKey;

import java.io.IOException;

public class WedgeO extends Configured implements Tool {

    private static Logger log = Logger.getLogger(WedgeO.class);

    public static class WedgeOMapper extends Mapper<OneNodeKey, IntDegreeArray, NullWritable, NullWritable> {

	private int wedge_num = 0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    wedge_num = context.getConfiguration().getInt("test.wedge_num", 2);
	}

	@Override
	protected void map(OneNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    if (wedge_num == 2) {
		if (value.size >= 2) {
		    context.getCounter("test", "core").increment(1);
		    context.getCounter("test", "list_size").increment(value.size);
		    context.getCounter("test", "pattern_size").increment(value.size * (value.size - 1) / 2);
		}
	    }

	    if (wedge_num == 4) {
		if (value.size >= 4) {
		    context.getCounter("test", "core").increment(1);
		    context.getCounter("test", "list_size").increment(value.size);
		    context.getCounter("test", "pattern_size").increment(
			    (long) value.size * (value.size - 1) * (value.size - 2) * (value.size - 3) / (2 * 3 * 4));
		}
	    }
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
	job.setJobName("Wedge");

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(NullOutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));

	job.setNumReduceTasks(0);

	job.setMapperClass(WedgeOMapper.class);

	boolean success = job.waitForCompletion(true);

	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	ToolRunner.run(conf, new WedgeO(), args);
	endTime = System.currentTimeMillis();
	log.info("[wedge] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
