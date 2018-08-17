package sAlg;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.THashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sData.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;



/**
 * <h1>
 * Count Three Triangle
 * </h1>
 * <ul>
 * 	<li>Pattern name: Three Triangle </li>
 * 	<li>Pattern: (0,1),(0,3),(0,4),(1,2),(1,3),(2,3),(3,4)  </li>
 * 	<li>Core: (0,1,3)</li>
 * 	<li>Non Core Node: [2,4]</li>
 * </ul>
 * @author zhanghao
 */

public class ThreeTriangle extends Configured implements Tool {

    private static Logger log = Logger.getLogger(ThreeTriangle.class);

    public static class ThreeTriangleMapper1
	    extends Mapper<TwoNodeKey, IntDegreeArray, PartitonKey, TwoNodeKeyIntDegreeArray> {

	private Integer p;
	private Integer m;
	private PartitonKey aKey = new PartitonKey();
	private TwoNodeKeyIntDegreeArray aValue = new TwoNodeKeyIntDegreeArray();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
	    m = conf.getInt("test.m", p / 4);

	}

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    aKey.setKey(key, (int) (Math.random() * p), p, m);
	    aValue.set(key, value);
	    context.write(aKey, aValue);
	}
    }

    public static class ThreeTriangleReducer1
	    extends Reducer<PartitonKey, TwoNodeKeyIntDegreeArray, NullWritable, NullWritable> {

	@Override
	protected void reduce(PartitonKey key, Iterable<TwoNodeKeyIntDegreeArray> values, Context context)
		throws IOException, InterruptedException {

	    // context.write(new IntWritable(key.key), new
	    // IntWritable(key.randomKey));

	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate");
	    String outputPath = path + "-" + key.key + "-" + key.randomKey;
	    FileSystem fileSystem = FileSystem.get(conf);
	    //
	    // Option optPath = SequenceFile.Writer.file(new Path(outputPath));
	    // Option optKey = SequenceFile.Writer.keyClass(TwoNodeKey.class);
	    // Option optVal =
	    // SequenceFile.Writer.valueClass(IntDegreeArray.class);

	    // writer = SequenceFile.createWriter(conf, optPath, optKey,
	    // optVal);

	    SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, new Path(outputPath),
		    TwoNodeKey.class, IntDegreeArray.class);

	    for (TwoNodeKeyIntDegreeArray testTwoNodeKeyIntDegreeArray : values) {
		TwoNodeKeyIntDegreeArray aCopied = WritableUtils.clone(testTwoNodeKeyIntDegreeArray,
			context.getConfiguration());
		TwoNodeKey outputKey = aCopied.key;
		IntDegreeArray outputValue = aCopied.array;
		writer.append(outputKey, outputValue);
	    }
	    writer.close();
	}
    }

    public static class ThreeTriangleMapper2
	    extends Mapper<TwoNodeKey, IntDegreeArray, PartitionTwoKey, TriangleValue> {

	private Integer p;
	private PartitionTwoKey aKey = new PartitionTwoKey();
	private TriangleValue aValue = new TriangleValue();
	private TwoNodeKey key1 = new TwoNodeKey();
	private TwoNodeKey key2 = new TwoNodeKey();
	private OneNodeKey partValue = new OneNodeKey();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
	}

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    int size = value.size;
	    for (int i = 0; i < size; ++i) {
		partValue.set(value.getNode(i), value.getDegree(i));

		if (key.node1 < value.getNode(i)) {
		    key1.setNode1(key.node1);
		    key1.setNode1Degree(key.node1Degree);
		    key1.setNode2(value.getNode(i));
		    key1.setNode2Degree(value.getDegree(i));
		} else {
		    key1.setNode2(key.node1);
		    key1.setNode2Degree(key.node1Degree);
		    key1.setNode1(value.getNode(i));
		    key1.setNode1Degree(value.getDegree(i));
		}

		if (key.node2 < value.getNode(i)) {
		    key2.setNode1(key.node2);
		    key2.setNode1Degree(key.node2Degree);
		    key2.setNode2(value.getNode(i));
		    key2.setNode2Degree(value.getDegree(i));
		} else {
		    key2.setNode2(key.node2);
		    key2.setNode2Degree(key.node2Degree);
		    key2.setNode1(value.getNode(i));
		    key2.setNode1Degree(value.getDegree(i));
		}

		aKey.setKey(key1, key2, p);
		aValue.set(partValue, key);
		context.write(aKey, aValue);
	    }
	}
    }

    public static class ThreeTriangleReducer2
	    extends Reducer<PartitionTwoKey, TriangleValue, NullWritable, NullWritable> {

	SequenceFile.Writer writer = null;

	@Override
	protected void reduce(PartitionTwoKey key, Iterable<TriangleValue> values, Context context)
		throws IOException, InterruptedException {

	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate2");
	    String outputPath = path + "-" + key.key1 + "-" + key.key2;
	    FileSystem fileSystem = null;

	    fileSystem = FileSystem.get(conf);

	    // 定义输出流（SequenceFile）

	    writer = SequenceFile.createWriter(fileSystem, conf, new Path(outputPath), TwoNodeKey.class,
		    OneNodeKey.class);

	    for (TriangleValue testTriangle : values) {
		TriangleValue outputResult = WritableUtils.clone(testTriangle, context.getConfiguration());

		writer.append(outputResult.node12, outputResult.node3);
	    }
	    IOUtils.closeStream(writer);

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

	}

    }

    public static class ThreeTriangleMapper3 extends Mapper<Object, Text, NullWritable, NullWritable> {

	private final OneNodeKey node1 = new OneNodeKey();
	private final OneNodeKey node2 = new OneNodeKey();
	private final OneNodeKey node3 = new OneNodeKey();

	private final TwoNodeKey leftEdge = new TwoNodeKey();
	private final TwoNodeKey rightEdge = new TwoNodeKey();

	private Integer p;
	private Integer m;
	private Integer f;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
	    m = conf.getInt("test.m", p / 4);
	    f = conf.getInt("test.f",2);
	}

	private List<Integer> getProblem(Text line) {
	    StringTokenizer st = new StringTokenizer(line.toString());

	    List<Integer> _problem = new ArrayList<Integer>();
	    try {
		for (;;) {
		    _problem.add(Integer.parseInt(st.nextToken()));
		}
	    } catch (Exception localException) {
		return _problem;
	    }
	}

	long Counter = 0;

		private boolean isFiltered(int node0, int node1, int node2, int node3, int node4){

			int centerTriangle = ((node0*31+node1)*31+node3) %10;
			int indexTriangle1 = ((node0*31+node3)*31+node4) %10;
			int indexTriangle2 = ((node1*31+node3)*31+node2) %10;

			return centerTriangle < f && indexTriangle1 < f && indexTriangle2 < f;
		}

	public void enumerate(int node0, int node1, int node2, int node3, int node4, Context context){
			if (isFiltered(node0,node1,node2,node3,node4)){
				++Counter;
			}
	}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			context.getCounter("test","enumeration").increment(Counter);
		}

		private void readTriangle(String path, Context context, int i, int j,
								  THashMap<TwoNodeKey, TIntArrayList> triangleList) throws IOException {
	    Configuration conf = context.getConfiguration();

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[i * m + j], conf);
	    TwoNodeKey key = (TwoNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    IntDegreeArray value = (IntDegreeArray) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    // long position = reader.getPosition();
	    while (reader.next(key, value)) {
		// String syncSeen = reader.syncSeen() ? "*" : "";
		// position = reader.getPosition();

		triangleList.put(WritableUtils.clone(key, context.getConfiguration()),
			new TIntArrayList(value.nodeArray));
	    }
	    IOUtils.closeStream(reader);
	}

	private void enumerateSubgraph(String path, Context context, int i, int j,
		THashMap<TwoNodeKey, TIntArrayList> triangleList) throws IOException {
		Boolean isEnumerating = context.getConfiguration().getBoolean("test.isEmitted", false);
		Configuration conf = context.getConfiguration();

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    reader = new SequenceFile.Reader(fs, new Path(path), conf);
	    TwoNodeKey key = (TwoNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    OneNodeKey value = (OneNodeKey) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    long position = reader.getPosition();
	    while (reader.next(key, value)) {
		String syncSeen = reader.syncSeen() ? "*" : "";
		position = reader.getPosition();

		node1.set(key.node1, key.node1Degree);
		node2.set(key.node2, key.node2Degree);
		node3.set(value.node, value.degree);

		if (node1.node < node3.node) {
		    leftEdge.set(node1, node3);
		} else {
		    leftEdge.set(node3, node1);
		}

		if (node2.node < node3.node) {
		    rightEdge.set(node2, node3);
		} else {
		    rightEdge.set(node3, node2);
		}

		TIntArrayList leftEdgeList = triangleList.get(leftEdge);
		TIntArrayList rightEdgeList = triangleList.get(rightEdge);

		long counter_intersectin = 0;

		for (int m = 0; m < leftEdgeList.size(); ++m) {
		    for (int n = 0; n < rightEdgeList.size(); ++n) {
			int leftPoint = leftEdgeList.get(m);
			int rightPoint = rightEdgeList.get(n);
			if (leftPoint == rightPoint) {
			    ++counter_intersectin;
			}
		    }
		}


			//enumerating
			if (isEnumerating) {
				for (int m = 0; m < leftEdgeList.size(); ++m) {
					for (int n = 0; n < rightEdgeList.size(); ++n) {
						int leftPoint = leftEdgeList.get(m);
						int rightPoint = rightEdgeList.get(n);
						if (leftPoint != rightPoint) {
							int node0 = key.node1;
							int node1 = leftPoint;
							int node2 = value.node;
							int node3 = rightPoint;
							int node4 = key.node2;

							if (node3 != node0 && node1 != node4){
								enumerate(node0, node1, node2, node3, node4, context);
							}
						}
					}
				}
			}



		if (leftEdgeList != null && rightEdgeList != null) {

		    // this counting is just a approximation not reliable
		    context.getCounter("test", "Result").increment((leftEdgeList.size() * rightEdgeList.size())
			    - leftEdgeList.size() - rightEdgeList.size() + 1 - counter_intersectin);
		}
	    }

	    // if (leftEdgeList != null && rightEdgeList != null) {
	    // //this counting is just a approximation not reliable
	    // vCounter += (((leftEdgeList.size() -
	    // 1)*(rightEdgeList.size()-1)/2)+1);
	    // }
	    // }
	    //
	    // context.getCounter("test","graph").increment(vCounter);
	    IOUtils.closeStream(reader);
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, NullWritable>.Context context)
		throws IOException, InterruptedException {
	    List<Integer> problems = getProblem(value);
	    THashMap<TwoNodeKey, TIntArrayList> triangleList = new THashMap<TwoNodeKey, TIntArrayList>();

	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate");

	    for (int i = 0; i < m; ++i) {
		String inputPath = path + "-" + problems.get(0) + "-" + i;
		readTriangle(inputPath, context, problems.get(0), i, triangleList);
	    }

	    for (int i = 0; i < m; ++i) {
		String inputPath = path + "-" + problems.get(1) + "-" + i;

		readTriangle(inputPath, context, problems.get(1), i, triangleList);
	    }

	    String path2 = conf.get("test.triangleIntermidiate2");
	    String inputPath2 = path2 + "-" + problems.get(0) + "-" + problems.get(1);
	    enumerateSubgraph(inputPath2, context, problems.get(0), problems.get(1), triangleList);

	}
    }

    private String createSeedFile(String seedFile, int p) throws IOException {
	Configuration conf = getConf();

	List<String> seedList = new ArrayList();

	FSDataOutputStream seedOut = FileSystem.get(conf).create(new Path(seedFile));

	for (int i = 0; i < p; i++) {
	    for (int j = 0; j < p; j++) {
		seedList.add(i + " " + j + "\n");
	    }
	}

	Collections.shuffle(seedList);

	System.out.printf("[Info] %d subproblem(s) to process\n", Integer.valueOf(seedList.size()));

	for (String s : seedList) {
	    seedOut.writeBytes(s);
	}

	seedOut.close();

	return seedFile;
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();

	FileSystem.get(conf).delete(new Path(args[0] + ".temp"));
	FileSystem.get(conf).delete(new Path(args[0] + ".temp2"));
	FileSystem.get(conf).delete(new Path(args[0] + ".seed"));

	// create seed file

	int p = conf.getInt("test.p", 40);
	int m = 10;
	String seed = args[0] + ".seed";
	createSeedFile(seed, p);

	int memory_size = conf.getInt("test.memory", 4000);
	int opts_size = (int) (memory_size * 0.8);
	String memory_opts = "-Xmx" + opts_size + "M";

	conf.setInt("mapreduce.map.memory.mb", memory_size);
	conf.set("mapreduce.map.java.opts", memory_opts);
	conf.setInt("mapreduce.reduce.memory.mb", memory_size);
	conf.set("mapreduce.reduce.java.opts", memory_opts);
	conf.set("test.triangleIntermidiate", args[0] + ".triangleIntermidiate/temp");
	conf.set("test.triangleIntermidiate2", args[0] + ".triangleIntermidiate2/temp");
	conf.setInt("test.p", p);
	conf.setInt("test.m", m);

	FileSystem.get(conf).delete(new Path(conf.get("test.triangleIntermidiate")));
	FileSystem.get(conf).delete(new Path(conf.get("test.triangleIntermidiate2")));

	// job1
	Job job1 = new Job(conf);
	job1.setJarByClass(getClass());
	job1.setJobName("ThreeTriangle1");

	job1.setInputFormatClass(SequenceFileInputFormat.class);

	job1.setMapperClass(ThreeTriangleMapper1.class);
	job1.setReducerClass(ThreeTriangleReducer1.class);

	FileInputFormat.setInputPaths(job1, args[0]);
	FileOutputFormat.setOutputPath(job1, new Path(args[0] + ".temp"));
	FileOutputFormat.setCompressOutput(job1, false);

	job1.setMapOutputKeyClass(PartitonKey.class);
	job1.setMapOutputValueClass(TwoNodeKeyIntDegreeArray.class);

	job1.setOutputKeyClass(NullWritable.class);
	job1.setOutputValueClass(NullWritable.class);

	job1.setNumReduceTasks(240);

	boolean success = job1.waitForCompletion(true);



	// job2

	Job job2 = new Job(conf);
	job2.setJarByClass(getClass());
	job2.setJobName("ThreeTriangle2");

	job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setOutputFormatClass(SequenceFileOutputFormat.class);
	job2.setMapperClass(ThreeTriangleMapper2.class);
	job2.setReducerClass(ThreeTriangleReducer2.class);

	FileInputFormat.setInputPaths(job2, args[0]);
	FileOutputFormat.setOutputPath(job2, new Path(args[0] + ".temp2"));

	job2.setMapOutputKeyClass(PartitionTwoKey.class);
	job2.setMapOutputValueClass(TriangleValue.class);

	job2.setOutputKeyClass(NullWritable.class);
	job2.setOutputValueClass(NullWritable.class);

		job2.setNumReduceTasks(240);

	success = job2.waitForCompletion(true);
	// job3

	String path = conf.get("test.triangleIntermidiate");

	for (int i = 0; i < p; ++i) {
	    for (int j = 0; j < m; ++j) {
		org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(path + "-" + i + "-" + j), getConf());
	    }
	}

	Job job3 = new Job(conf);

	job3.setJarByClass(getClass());
	job3.setJobName("ThreeTriangle");

	job3.getConfiguration().setInt("mapred.line.input.format.linespermap", p / 5);

	FileInputFormat.setInputPaths(job3, new Path(seed));

	job3.setInputFormatClass(NLineInputFormat.class);
	job3.setOutputFormatClass(NullOutputFormat.class);

	job3.setNumReduceTasks(0);

	job3.setMapperClass(ThreeTriangleMapper3.class);

	job3.waitForCompletion(true);

	return 0;
    }

    public static void main(String[] args) throws Exception {
	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();

	JobConf conf = new JobConf();
	long milliSeconds = 10000 * 60 * 60; // <default is 600000, likewise can
					     // give any value)
	conf.setLong("mapred.task.timeout", milliSeconds);
	ToolRunner.run(conf, new ThreeTriangle(), args);

	endTime = System.currentTimeMillis();
	log.info("[three triangle] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
