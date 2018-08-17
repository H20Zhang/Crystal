package sPreprocess.sTool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import sData.EdgeWritable;

public class InducedSubgraphGeneratorB extends Configured implements Tool {
    public static class InducedSubgraphGeneratorBMapper
	    extends Mapper<IntWritable, IntWritable, EdgeWritable, IntWritable> {

	private final EdgeWritable akey = new EdgeWritable();

	@Override
	protected void map(IntWritable key, IntWritable value, Context context)
		throws IOException, InterruptedException {
	    int a = key.get();
	    int b = value.get();
	    if (a < b) {
		akey.setNode1(a);
		akey.setNode2(b);
	    } else {
		akey.setNode1(b);
		akey.setNode2(a);
	    }
	    context.write(akey, key);
	}
    }

    public static class InducedSubgraphGeneratorBReducer
	    extends Reducer<EdgeWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(EdgeWritable key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    int counter = 0;
	    for (IntWritable value : values) {
		counter++;
	    }

	    if (counter == 2) {
		context.write(new IntWritable(key.getNode1()), new IntWritable(key.getNode2()));
	    }
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf);

	job.setJarByClass(InducedSubgraphGeneratorManager.class);
	job.setJobName("Induce subgraph");
	job.setNumReduceTasks(240);

	FileInputFormat.setInputPaths(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]));

	job.setInputFormatClass(SequenceFileInputFormat.class);

	job.setMapperClass(InducedSubgraphGeneratorBMapper.class);
	job.setReducerClass(InducedSubgraphGeneratorBReducer.class);

	job.setMapOutputKeyClass(EdgeWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }
}
