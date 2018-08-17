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



/**
 * 
 * 
 * 
 * <h1>
 * Count Chordal Square Plus
 * </h1>
 * <ul>
 * 	<li>Pattern name: Chordal Square Plus </li>
 * 	<li>Pattern: (0,1),(0,2),(0,3),(0,4),(1,2),(2,3),(2,4)  </li>
 * 	<li>Core: (0,2)</li>
 * 	<li>Non Core Node: [1,3,4]</li>
 * </ul>
 * 
 * @author zhanghao
 */

public class ChordalSquarePlus extends Configured implements Tool {

    private static Logger log = Logger.getLogger(ChordalSquarePlus.class);

    public static class ChordalSquareMapper extends Mapper<TwoNodeKey, IntDegreeArray, NullWritable, NullWritable> {
	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    for (int i = 0; i < value.size; ++i) {
		for (int j = i; j < value.size; ++j) {
		    int node1 = value.getNode(i);
		    int node2 = value.getNode(j);
		}
	    }
	    if (value.size >= 3) {
		context.getCounter("test", "chordal_square")
			.increment(value.size * (value.size - 1) * (value.size - 2) / 6);
	    }
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
	job.setJobName("Chordal Square Plus");

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(NullOutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));

	job.setNumReduceTasks(0);

	job.setMapperClass(ChordalSquareMapper.class);

	boolean success = job.waitForCompletion(true);

	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	ToolRunner.run(conf, new ChordalSquarePlus(), args);
	endTime = System.currentTimeMillis();
	log.info("[chordal square] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
