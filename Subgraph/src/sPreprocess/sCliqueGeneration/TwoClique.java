package sPreprocess.sCliqueGeneration;

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
import org.apache.hadoop.util.ToolRunner;

import java.util.List;
import java.util.ArrayList;

import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.TwoNodeKey;



/**
 * Pattern name: Line <br>
 * Pattern: (0,1)
 * 
 * @author zhanghao
 */
public class TwoClique extends Configured implements Tool {
    public static class TwoCliqueMapper extends Mapper<IntWritable, TwoNodeKey, OneNodeKey, OneNodeKey> {

	private final OneNodeKey aKey = new OneNodeKey();
	private final OneNodeKey aValue = new OneNodeKey();

	@Override
	protected void map(IntWritable key, TwoNodeKey value,
		Mapper<IntWritable, TwoNodeKey, OneNodeKey, OneNodeKey>.Context context)
		throws IOException, InterruptedException {
	    aKey.set(value.getNode1(), value.getNode1Degree());
	    aValue.set(value.getNode2(), value.getNode2Degree());

	    context.write(aKey, aValue);

	    aKey.set(value.getNode2(), value.getNode2Degree());
	    aValue.set(value.getNode1(), value.getNode1Degree());

	    context.write(aKey, aValue);
	}

    }

    public static class TwoCliqueReducer extends Reducer<OneNodeKey, OneNodeKey, OneNodeKey, IntDegreeArray> {

	private final List<OneNodeKey> list = new ArrayList<OneNodeKey>();

	@Override
	protected void reduce(OneNodeKey key, Iterable<OneNodeKey> values, Context context)
		throws IOException, InterruptedException {
	    for (OneNodeKey oneNodeKey : values) {
		list.add(new OneNodeKey(oneNodeKey));
	    }

	    IntDegreeArray outputValue = new IntDegreeArray(list.size());
	    int pos = 0;
	    for (OneNodeKey oneNodeKey : list) {
		outputValue.addNodeAndDegree(pos++, oneNodeKey.getNode(), oneNodeKey.getDegree());
	    }

	    context.write(key, outputValue);
	    list.clear();
	}
    }

    @Override
    public int run(String[] args) throws Exception {

	Configuration conf = getConf();
	Job job = new Job(conf);

	job.setJarByClass(getClass());
	job.setJobName("Two Clique");

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setMapperClass(TwoClique.TwoCliqueMapper.class);
	job.setReducerClass(TwoClique.TwoCliqueReducer.class);

	job.setMapOutputKeyClass(OneNodeKey.class);
	job.setMapOutputValueClass(OneNodeKey.class);

	job.setOutputKeyClass(OneNodeKey.class);
	job.setOutputValueClass(IntDegreeArray.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }

    public static void main(String[] args) {
	Configuration conf = new Configuration();

	conf.set("mapreduce.reduce.memory.mb", "4096");
	// Run map1 and reduce1
	int exitCode = 0;
	try {
	    exitCode = ToolRunner.run(conf, new TwoClique(), args);
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	System.exit(exitCode);
    }

}
