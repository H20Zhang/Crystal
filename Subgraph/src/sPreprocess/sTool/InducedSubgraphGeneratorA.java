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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

public class InducedSubgraphGeneratorA extends Configured implements Tool {
    public static class InducedSubgraphGeneratorAMapper
	    extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void map(IntWritable key, IntWritable value, Context context)
		throws IOException, InterruptedException {
	    context.write(key, value);
	    context.write(value, key);
	}
    }

    public static class InducedSubgraphGeneratorAReducer
	    extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private Integer percent;

	@Override
	protected void setup(Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
		throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    Configuration conf = context.getConfiguration();
	    percent = new Integer(conf.get("percent"));
	}

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {

	    double number = Math.random() * 100;
	    if (number > (100 - percent)) {
		context.getCounter("node_number", "total").increment(1);
		for (IntWritable value : values) {
		    // context.getCounter("total",
		    // conf.get("mapred.task.partition")).increment(1);
		    context.write(key, value);
		}
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

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setMapperClass(InducedSubgraphGeneratorAMapper.class);
	job.setReducerClass(InducedSubgraphGeneratorAReducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }
}
