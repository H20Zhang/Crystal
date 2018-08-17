package sAlg;

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
 * <h1>
 * Count Chordal Square
 * </h1>
 * <ul>
 * 	<li>Pattern name: Chordal Square </li>
 * 	<li>Pattern: (0,1),(0,2),(0,3),(1,2),(2,3)  </li>
 * 	<li>Core: (0,2)</li>
 * 	<li>Non Core Node: [1,3]</li>
 * </ul>
 * 
 * @author zhanghao
 */
public class ChordalSquare extends Configured implements Tool {

    private static Logger log = Logger.getLogger(ChordalSquare.class);

    public static class ChordalSquareMapper extends Mapper<TwoNodeKey, IntDegreeArray, NullWritable, NullWritable> {

    	long Counter = 0;

    	public void enumerate(int node0, int node1, int node2, int node3, Context context){
    	++Counter;
//    		context.getCounter("test", "enumeration").increment(1);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			context.getCounter("test", "enumeration").increment(Counter);
		}

		@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    if (value.size >= 2 && key.node1 < key.node2) {

	    	//counting
		context.getCounter("test", "core").increment(1);
		context.getCounter("test","part").increment(value.size);
		context.getCounter("test", "subgraph").increment(value.size * (value.size - 1) / 2);

            Boolean isEnumerating = context.getConfiguration().getBoolean("test.isEmitted", false);

            if (isEnumerating) {
                //enumeration
                for (int i = 0; i < value.nodeArray.length; i++) {
                    for (int j = i + 1; j < value.nodeArray.length; j++) {
                        int node0 = key.node1;
                        int node1 = value.nodeArray[i];
                        int node2 = key.node2;
                        int node3 = value.nodeArray[j];
                        enumerate(node0, node1, node2, node3, context);
                    }
                }
			}


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
	job.setJobName("Chordal Square");

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
	
//	conf.setNumMapTasks(12);
//	conf.setNumReduceTasks(12);
//	conf.set("mapreduce.local.map.tasks.maximum", "12");
//	conf.set("mapreduce.local.reduce.tasks.maximum", "12");
//	conf.set("mapred.job.tracker", "local");
//	conf.set("mapreduce.framework.name", "local");
	
	ToolRunner.run(conf, new ChordalSquare(), args);
	endTime = System.currentTimeMillis();
	log.info("[chordal square] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
