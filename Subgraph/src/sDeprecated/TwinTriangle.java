package sDeprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.TwoNodeKey;

import java.io.IOException;

public class TwinTriangle extends Configured implements Tool {

    private static Logger log = Logger.getLogger(TwinTriangle.class);

    public static class TwinTriangleMapper extends Mapper<TwoNodeKey, IntDegreeArray, OneNodeKey, TwoNodeKey> {

	private final OneNodeKey aKey = new OneNodeKey();
	private final TwoNodeKey aValue = new TwoNodeKey();

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    for (int i = 0; i < value.size; ++i) {
		aKey.set(value.getNode(i), value.getDegree(i));
		context.write(aKey, key);
	    }
	}
    }

    public static class TwinTriangleReducer extends Reducer<OneNodeKey, TwoNodeKey, NullWritable, NullWritable> {
	@Override
	protected void reduce(OneNodeKey key, Iterable<TwoNodeKey> values, Context context)
		throws IOException, InterruptedException {

	    long count = 0;
	    for (TwoNodeKey twoNodeKey : values) {
		count = count + 1;
	    }

	    context.getCounter("test", "TwinTriangle").increment(count * (count - 1) / 2);
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

	job.setMapperClass(TwinTriangleMapper.class);
	job.setReducerClass(TwinTriangleReducer.class);

	job.setMapOutputKeyClass(OneNodeKey.class);
	job.setMapOutputValueClass(TwoNodeKey.class);

	boolean success = job.waitForCompletion(true);

	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	ToolRunner.run(conf, new TwinTriangle(), args);
	endTime = System.currentTimeMillis();
	log.info("[TwinTriangle] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
