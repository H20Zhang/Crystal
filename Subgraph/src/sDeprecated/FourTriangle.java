package sDeprecated;

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
 * 
 * 
 * <h1>
 * Count Four Triangle
 * </h1>
 * <ul>
 * 	<li>Pattern name: Four Triangle </li>
 * 	<li>Pattern: (0,1),(0,2),(0,4),(0,5),(1,2),(2,3),(2,4),(3,4),(4,5)  </li>
 * 	<li>Core: (0,2,4)</li>
 * 	<li>Non Core Node: [1,3,5]</li>
 * </ul>
 * @author zhanghao
 * @version NotTested
 */

public class FourTriangle extends Configured implements Tool {

    private static Logger log = Logger.getLogger(FourTriangle.class);

    public static class FourTriangleMapper1
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

    public static class FourTriangleReducer1
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

    public static class FourTriangleMapper2
	    extends Mapper<TwoNodeKey, IntDegreeArray, PartitionThreeKey, TriangleValue> {

	private Integer p;
	private PartitionThreeKey aKey = new PartitionThreeKey();
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

		aKey.setKey(key1, key2, key, p);
		aValue.set(partValue, key);
		context.write(aKey, aValue);
	    }
	}
    }

    public static class FourTriangleReducer2
	    extends Reducer<PartitionThreeKey, TriangleValue, NullWritable, NullWritable> {

	SequenceFile.Writer writer = null;

	@Override
	protected void reduce(PartitionThreeKey key, Iterable<TriangleValue> values, Context context)
		throws IOException, InterruptedException {

	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate2");
	    String outputPath = path + "-" + key.key1 + "-" + key.key2 + "-" + key.key3;
	    FileSystem fileSystem = null;

	    fileSystem = FileSystem.get(conf);

	    // 定义输出流（SequenceFile）

	    writer = SequenceFile.createWriter(fileSystem, conf, new Path(outputPath), TwoNodeKey.class,
		    OneNodeKey.class);

	    for (TriangleValue triangle : values) {
		TriangleValue outputResult = WritableUtils.clone(triangle, context.getConfiguration());

		writer.append(outputResult.node12, outputResult.node3);
	    }
	    IOUtils.closeStream(writer);
	}
    }

    public static class FourTriangleMapper3 extends Mapper<Object, Text, NullWritable, NullWritable> {

	private final OneNodeKey node1 = new OneNodeKey();
	private final OneNodeKey node2 = new OneNodeKey();
	private final OneNodeKey node3 = new OneNodeKey();

	private final TwoNodeKey leftEdge = new TwoNodeKey();
	private final TwoNodeKey rightEdge = new TwoNodeKey();

	private Integer p;
	private Integer m;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
	    m = conf.getInt("test.m", p / 4);
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

	private void enumerateSubgraph(String path, Context context, int i, int j, int h,
		THashMap<TwoNodeKey, TIntArrayList> triangleList) {

	    try {
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
		    TIntArrayList bottomEdgeList = triangleList.get(key);

		    for (int m = 0; m < leftEdgeList.size(); ++m) {
			for (int n = 0; n < rightEdgeList.size(); ++n) {
			    for (int q = 0; q < bottomEdgeList.size(); q++) {
				int leftPoint = leftEdgeList.get(m);
				int rightPoint = rightEdgeList.get(n);
				int bottomPoint = bottomEdgeList.get(q);
				if (leftPoint != rightPoint && leftPoint != bottomPoint && rightPoint != bottomPoint
					&& leftPoint != node1.node && rightPoint != node2.node
					&& bottomPoint != value.getNode()) {

				}
			    }
			}
		    }

		    if (leftEdgeList != null && rightEdgeList != null) {
			context.getCounter("test", "Result")
				.increment((leftEdgeList.size() - 1) * (rightEdgeList.size() - 2) / 2);
		    }
		}
		IOUtils.closeStream(reader);
	    } catch (Exception e) {

	    }

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

	    for (int i = 0; i < m; ++i) {
		String inputPath = path + "-" + problems.get(2) + "-" + i;
		readTriangle(inputPath, context, problems.get(2), i, triangleList);
	    }

	    String path2 = conf.get("test.triangleIntermidiate2");
	    String inputPath2 = path2 + "-" + problems.get(0) + "-" + problems.get(1) + "-" + problems.get(2);
	    enumerateSubgraph(inputPath2, context, problems.get(0), problems.get(1), problems.get(2), triangleList);

	    triangleList.clear();
	}
    }

    private String createSeedFile(String seedFile, int p) throws IOException {
	Configuration conf = getConf();

	List<String> seedList = new ArrayList();

	FSDataOutputStream seedOut = FileSystem.get(conf).create(new Path(seedFile));

	for (int i = 0; i < p; i++) {
	    for (int j = 0; j < p; j++) {
		for (int h = 0; h < p; h++) {
		    seedList.add(i + " " + j + " " + h + "\n");
		}
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
	int p = 20;
	int m = 20;
	String seed = args[0] + ".seed";
	createSeedFile(seed, p);

	conf.set("mapreduce.map.memory.mb", "4000");
	conf.set("mapreduce.map.java.opts", "-Xmx4000M");
	conf.set("mapreduce.reduce.memory.mb", "4000");
	conf.set("mapreduce.reduce.java.opts", "-Xmx4000M");
	conf.set("test.triangleIntermidiate", args[0] + ".triangleIntermidiate/temp");
	conf.set("test.triangleIntermidiate2", args[0] + ".triangleIntermidiate2/temp");
	conf.setInt("test.p", p);
	conf.setInt("test.m", m);

	FileSystem.get(conf).delete(new Path(conf.get("test.triangleIntermidiate")));
	FileSystem.get(conf).delete(new Path(conf.get("test.triangleIntermidiate2")));

	// job1
	Job job1 = new Job(conf);
	job1.setJarByClass(getClass());
	job1.setJobName("Preprocess1");

	job1.setInputFormatClass(SequenceFileInputFormat.class);

	job1.setMapperClass(FourTriangleMapper1.class);
	job1.setReducerClass(FourTriangleReducer1.class);

	FileInputFormat.setInputPaths(job1, args[0]);
	FileOutputFormat.setOutputPath(job1, new Path(args[0] + ".temp"));
	FileOutputFormat.setCompressOutput(job1, false);

	job1.setMapOutputKeyClass(PartitonKey.class);
	job1.setMapOutputValueClass(TwoNodeKeyIntDegreeArray.class);

	job1.setOutputKeyClass(NullWritable.class);
	job1.setOutputValueClass(NullWritable.class);

	boolean success = job1.waitForCompletion(true);

	// job2

	Job job2 = new Job(conf);
	job2.setJarByClass(getClass());
	job2.setJobName("Preprocess2");

	job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setOutputFormatClass(SequenceFileOutputFormat.class);
	job2.setMapperClass(FourTriangleMapper2.class);
	job2.setReducerClass(FourTriangleReducer2.class);

	FileInputFormat.setInputPaths(job2, args[0]);
	FileOutputFormat.setOutputPath(job2, new Path(args[0] + ".temp2"));

	job2.setMapOutputKeyClass(PartitionThreeKey.class);
	job2.setMapOutputValueClass(TriangleValue.class);

	job2.setOutputKeyClass(NullWritable.class);
	job2.setOutputValueClass(NullWritable.class);

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
	job3.setJobName("Preprocess");

	job3.getConfiguration().setInt("mapred.line.input.format.linespermap", p / 5);

		FileInputFormat.setInputPaths(job3, new Path(seed));

	job3.setInputFormatClass(NLineInputFormat.class);
	job3.setOutputFormatClass(NullOutputFormat.class);

	job3.setNumReduceTasks(0);

	job3.setMapperClass(FourTriangleMapper3.class);

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
	ToolRunner.run(conf, new FourTriangle(), args);

	endTime = System.currentTimeMillis();
	log.info("[four triangle] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }
}
