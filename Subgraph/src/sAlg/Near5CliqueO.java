package sAlg;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.THashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
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
 * Output Near5Clique
 * </h1>
 * <ul>
 * 	<li>Pattern name: Near5Clique </li>
 * 	<li>Pattern: (0,1),(0,2),(0,4),(1,2),(1,4),(2,3),(2,4),(3,4)  </li>
 * 	<li>Core: (0,2,4)</li>
 * 	<li>Non Core Node: [1,3]</li>
 * </ul>
 * @author zhanghao
 */

public class Near5CliqueO extends Configured implements Tool {

    private static Logger log = Logger.getLogger(Near5CliqueO.class);

    public static class Near5CliqueNMapper1
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

    public static class Near5CliqueNReducer1
	    extends Reducer<PartitonKey, TwoNodeKeyIntDegreeArray, NullWritable, NullWritable> {

	boolean partialGreater(OneNodeKey lhs, OneNodeKey rhs) {
	    if (lhs.degree > rhs.degree) {
		return true;
	    }

	    if (lhs.degree < rhs.degree) {
		return false;
	    }

	    if (lhs.node > rhs.node) {
		return true;
	    }

	    if (lhs.node < rhs.node) {
		return false;
	    }

	    return false;
	}

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
	    Option optPath = SequenceFile.Writer.file(new Path(outputPath));
	    Option optKey = SequenceFile.Writer.keyClass(TwoNodeKey.class);
	    Option optVal = SequenceFile.Writer.valueClass(IntDegreeArray.class);
//	    Option optCompress = SequenceFile.Writer.compression(CompressionType.BLOCK, new SnappyCodec());

	    SequenceFile.Writer writer = SequenceFile.createWriter(conf, optPath, optKey, optVal);

	    // SequenceFile.Writer writer =
	    // SequenceFile.createWriter(fileSystem, conf, new Path(outputPath),
	    // TwoNodeKey.class,IntDegreeArray.class);

	    for (TwoNodeKeyIntDegreeArray testTwoNodeKeyIntDegreeArray : values) {
		TwoNodeKeyIntDegreeArray aCopied = WritableUtils.clone(testTwoNodeKeyIntDegreeArray,
			context.getConfiguration());
		TwoNodeKey outputKey = aCopied.key;

		OneNodeKey outputKey1 = new OneNodeKey();
		outputKey1.node = outputKey.getNode1();
		outputKey1.degree = outputKey.getNode1Degree();

		OneNodeKey outputKey2 = new OneNodeKey();
		outputKey2.node = outputKey.getNode2();
		outputKey2.degree = outputKey.getNode2Degree();

		ArrayList<OneNodeKey> tempList = new ArrayList<OneNodeKey>();
		for (int i = 0; i < aCopied.array.size; ++i) {
		    OneNodeKey tempNode = new OneNodeKey();
		    tempNode.node = aCopied.array.getNode(i);
		    tempNode.degree = aCopied.array.getDegree(i);

		    // if (partialGreater(tempNode, outputKey1) &&
		    // partialGreater(tempNode, outputKey2)) {
		    // tempList.add(tempNode);
		    // }
		    tempList.add(tempNode);
		}
		Collections.sort(tempList);
		tempList.trimToSize();

		IntDegreeArray outputValue = new IntDegreeArray(tempList.size());
		for (int i = 0; i < tempList.size(); ++i) {
		    outputValue.addNodeAndDegree(i, tempList.get(i).getNode(), tempList.get(i).getDegree());
		}

		writer.append(outputKey, outputValue);
	    }
	    writer.close();
	}
    }

    public static class Near5CliqueNMapper2 extends Mapper<TwoNodeKey, IntDegreeArray, PartitionTwoKey, TriangleValue> {

	private Integer p;
	private PartitionTwoKey aKey = new PartitionTwoKey();
	private TriangleValue aValue = new TriangleValue();
	private TwoNodeKey key1 = new TwoNodeKey();
	private TwoNodeKey key2 = new TwoNodeKey();
	private OneNodeKey partValue = new OneNodeKey();

	boolean partialGreater(OneNodeKey lhs, OneNodeKey rhs) {
	    if (lhs.degree > rhs.degree) {
		return true;
	    }

	    if (lhs.degree < rhs.degree) {
		return false;
	    }

	    if (lhs.node > rhs.node) {
		return true;
	    }

	    if (lhs.node < rhs.node) {
		return false;
	    }

	    return false;
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
	}

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    int size = value.size;
	    OneNodeKey oneNode1 = new OneNodeKey();
	    oneNode1.set(key.getNode1(), key.getNode1Degree());

	    OneNodeKey oneNode2 = new OneNodeKey();
	    oneNode2.set(key.getNode2(), key.getNode2Degree());

	    for (int i = 0; i < size; ++i) {
		partValue.set(value.getNode(i), value.getDegree(i));

		if (true) {

		    // key1.setNode1(key.node1);
		    // key1.setNode1Degree(key.node1Degree);
		    // key1.setNode2(key.node2);
		    // key1.setNode2Degree(key.node2Degree);
		    //
		    // key2.setNode1(key.node2);
		    // key2.setNode1Degree(key.node2Degree);
		    // key2.setNode2(value.getNode(i));
		    // key2.setNode2Degree(value.getDegree(i));

		    // if (key.node1 < value.getNode(i)) {
		    // key1.setNode1(key.node1);
		    // key1.setNode1Degree(key.node1Degree);
		    // key1.setNode2(key.node2);
		    // key1.setNode2Degree(key.node2Degree);
		    // } else {
		    // key1.setNode2(key.node1);
		    // key1.setNode2Degree(key.node1Degree);
		    // key1.setNode1(value.getNode(i));
		    // key1.setNode1Degree(value.getDegree(i));
		    // }

		    key1.setNode1(key.node1);
		    key1.setNode1Degree(key.node1Degree);
		    key1.setNode2(key.node2);
		    key1.setNode2Degree(key.node2Degree);

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
    }

    public static class Near5CliqueNReducer2
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
	    Option optPath = SequenceFile.Writer.file(new Path(outputPath));
	    Option optKey = SequenceFile.Writer.keyClass(TwoNodeKey.class);
	    Option optVal = SequenceFile.Writer.valueClass(OneNodeKey.class);
//	    Option optCompress = SequenceFile.Writer.compression(CompressionType.BLOCK, new SnappyCodec());

	    SequenceFile.Writer writer = SequenceFile.createWriter(conf, optPath, optKey, optVal);
	    // writer = SequenceFile.createWriter(fileSystem, conf, new
	    // Path(outputPath), TwoNodeKey.class,OneNodeKey.class);

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

    public static class Near5CliqueNMapper3 extends Mapper<Object, Text, NullWritable, NullWritable> {

	private final OneNodeKey node1 = new OneNodeKey();
	private final OneNodeKey node2 = new OneNodeKey();
	private final OneNodeKey node3 = new OneNodeKey();

	private final TwoNodeKey leftEdge = new TwoNodeKey();
	private final TwoNodeKey rightEdge = new TwoNodeKey();

	private final OneNodeKey rightNode = new OneNodeKey();

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

	private void enumerateSubgraph(String path, Context context, int i, int j,
		THashMap<TwoNodeKey, TIntArrayList> triangleList) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    reader = new SequenceFile.Reader(fs, new Path(path), conf);
	    TwoNodeKey key = (TwoNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    OneNodeKey value = (OneNodeKey) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    // long position = reader.getPosition();
	    while (reader.next(key, value)) {
		// String syncSeen = reader.syncSeen() ? "*" : "";
		// position = reader.getPosition();

		node1.set(key.node1, key.node1Degree);
		node2.set(key.node2, key.node2Degree);
		node3.set(value.node, value.degree);

		// if (node1.node < node2.node) {
		// leftEdge.set(node1, node2);
		// } else {
		// leftEdge.set(node2, node1);
		// }
		leftEdge.set(node1, node2);

		rightNode.set(node3.node, node3.degree);

		if (node2.node < node3.node) {
		    rightEdge.set(node2, node3);
		} else {
		    rightEdge.set(node3, node2);
		}
		// rightEdge.set(node2, node3);

		TIntArrayList leftEdgeList = triangleList.get(leftEdge);
		TIntArrayList rightEdgeList = triangleList.get(rightEdge);
		if (leftEdgeList != null && rightEdgeList != null) {	
		long counter_intersectin = 0;
		long leftEdgeListSizeN = leftEdgeList.size() - 2;

		counter_intersectin = intersect(leftEdgeList, rightEdgeList, rightNode.node);

			    
		    context.getCounter("test", "core").increment(1);
		    context.getCounter("test","part").increment(leftEdgeListSizeN+counter_intersectin);
		    context.getCounter("test", "subgraph").increment(counter_intersectin * leftEdgeListSizeN);
		}

	    }

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
	    triangleList.compact();
	    String path2 = conf.get("test.triangleIntermidiate2");
	    String inputPath2 = path2 + "-" + problems.get(0) + "-" + problems.get(1);
	    enumerateSubgraph(inputPath2, context, problems.get(0), problems.get(1), triangleList);

	}

	private long intersect(TIntArrayList uN, TIntArrayList vN, int node) throws IOException, InterruptedException {
	    if ((uN == null) || (vN == null)) {
		return 0L;
	    }

	    long count = 0L;
	    int uCur = 0;
	    int vCur = 0;
	    long uD = uN.size();
	    long vD = vN.size();

	    while ((uCur < uD) && (vCur < vD)) {

		if (uN.getQuick(uCur) < vN.getQuick(vCur)) {
		    uCur++;
		} else if (vN.getQuick(vCur) < uN.getQuick(uCur)) {
		    vCur++;
		} else {
		    if (uN.getQuick(uCur) < node) {
			count += 1L;
		    }

		    uCur++;
		    vCur++;
		}
	    }

	    return count;
	}

    }

    public static class Near5CliqueNOMapper3 extends Mapper<Object, Text, ThreeNodeKey, TwoIntDegreeArray> {

	private final OneNodeKey node1 = new OneNodeKey();
	private final OneNodeKey node2 = new OneNodeKey();
	private final OneNodeKey node3 = new OneNodeKey();

	private final TwoNodeKey leftEdge = new TwoNodeKey();
	private final TwoNodeKey rightEdge = new TwoNodeKey();

	private final OneNodeKey rightNode = new OneNodeKey();

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

	private void enumerateSubgraph(String path, Context context, int i, int j,
		THashMap<TwoNodeKey, TIntArrayList> triangleList) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    reader = new SequenceFile.Reader(fs, new Path(path), conf);
	    TwoNodeKey key = (TwoNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    OneNodeKey value = (OneNodeKey) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    // long position = reader.getPosition();
	    while (reader.next(key, value)) {
		// String syncSeen = reader.syncSeen() ? "*" : "";
		// position = reader.getPosition();

		node1.set(key.node1, key.node1Degree);
		node2.set(key.node2, key.node2Degree);
		node3.set(value.node, value.degree);

		// if (node1.node < node2.node) {
		// leftEdge.set(node1, node2);
		// } else {
		// leftEdge.set(node2, node1);
		// }
		leftEdge.set(node1, node2);

		rightNode.set(node3.node, node3.degree);

		if (node2.node < node3.node) {
		    rightEdge.set(node2, node3);
		} else {
		    rightEdge.set(node3, node2);
		}
		// rightEdge.set(node2, node3);

		TIntArrayList leftEdgeList = triangleList.get(leftEdge);
		TIntArrayList rightEdgeList = triangleList.get(rightEdge);

		TIntArrayList counter_intersectin = intersect(leftEdgeList, rightEdgeList, rightNode.node);

		IntDegreeArray array1 = new IntDegreeArray(leftEdgeList.size());
		IntDegreeArray array2 = new IntDegreeArray(counter_intersectin.size());

		for (int n = 0; n < array1.size; ++n) {
		    array1.addNodeAndDegree(n, leftEdgeList.get(n), leftEdgeList.get(n));
		}

		for (int n = 0; n < array2.size; ++n) {
		    array2.addNodeAndDegree(n, counter_intersectin.get(n), counter_intersectin.get(n));
		}

		ThreeNodeKey outputKey = new ThreeNodeKey();
		outputKey.set(node1, node2, node3);

		TwoIntDegreeArray outputValue = new TwoIntDegreeArray();
		outputValue.array1 = array1;
		outputValue.array2 = array2;

		if (leftEdgeList != null && rightEdgeList != null) {
		    context.write(outputKey, outputValue);
		}
	    }

	    IOUtils.closeStream(reader);
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
	    triangleList.compact();
	    String path2 = conf.get("test.triangleIntermidiate2");
	    String inputPath2 = path2 + "-" + problems.get(0) + "-" + problems.get(1);
	    enumerateSubgraph(inputPath2, context, problems.get(0), problems.get(1), triangleList);

	}

	private TIntArrayList intersect(TIntArrayList uN, TIntArrayList vN, int node)
		throws IOException, InterruptedException {

	    TIntArrayList tempList = new TIntArrayList();

	    if ((uN == null) || (vN == null)) {
		return null;
	    }

	    long count = 0L;
	    int uCur = 0;
	    int vCur = 0;
	    long uD = uN.size();
	    long vD = vN.size();

	    while ((uCur < uD) && (vCur < vD)) {

		if (uN.getQuick(uCur) < vN.getQuick(vCur)) {
		    uCur++;
		} else if (vN.getQuick(vCur) < uN.getQuick(uCur)) {
		    vCur++;
		} else {
		    if (vN.get(vCur) < node) {
			tempList.add(vN.getQuick(vCur));
		    }

		    uCur++;
		    vCur++;
		}
	    }

	    return tempList;
	}

    }

    private int createSeedFile(String seedFile, int p) throws IOException {
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

	return seedList.size();
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();

	FileSystem.get(conf).delete(new Path(args[0] + ".temp"));
	FileSystem.get(conf).delete(new Path(args[0] + ".temp2"));
	FileSystem.get(conf).delete(new Path(args[0] + ".seed"));
	FileSystem.get(conf).delete(new Path(args[0] + ".temp3"));

	// create seed file
	// p need to be 4 * x
	Boolean isOutput = conf.getBoolean("test.isOutput", false);

	int p = conf.getInt("test.p", 20);
	int m = 10;
	String seed = args[0] + ".seed";
	int seedListSize = createSeedFile(seed, p);

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

	FileSystem.get(conf).delete(new Path(args[0] + ".triangleIntermidiate"));
	FileSystem.get(conf).delete(new Path(args[0] + ".triangleIntermidiate2"));

	// job1
	Job job1 = new Job(conf);
	job1.setJarByClass(getClass());
	job1.setJobName("Near5CliqueO1");

	job1.setInputFormatClass(SequenceFileInputFormat.class);

	job1.setMapperClass(Near5CliqueNMapper1.class);
	job1.setReducerClass(Near5CliqueNReducer1.class);

	FileInputFormat.setInputPaths(job1, args[0]);
	FileOutputFormat.setOutputPath(job1, new Path(args[0] + ".temp"));
//	FileOutputFormat.setCompressOutput(job1, false);

	job1.setMapOutputKeyClass(PartitonKey.class);
	job1.setMapOutputValueClass(TwoNodeKeyIntDegreeArray.class);

	job1.setOutputKeyClass(NullWritable.class);
	job1.setOutputValueClass(NullWritable.class);

	boolean success = job1.waitForCompletion(true);

	// job2

	Job job2 = new Job(conf);
	job2.setJarByClass(getClass());
	job2.setJobName("Near5CliqueO2");

	job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setOutputFormatClass(SequenceFileOutputFormat.class);
	job2.setMapperClass(Near5CliqueNMapper2.class);
	job2.setReducerClass(Near5CliqueNReducer2.class);

	FileInputFormat.setInputPaths(job2, args[0]);
	FileOutputFormat.setOutputPath(job2, new Path(args[0] + ".temp2"));

	job2.setMapOutputKeyClass(PartitionTwoKey.class);
	job2.setMapOutputValueClass(TriangleValue.class);

	job2.setOutputKeyClass(NullWritable.class);
	job2.setOutputValueClass(NullWritable.class);

	success = job2.waitForCompletion(true);
	// job3

	if (isOutput == false) {
	    String path = conf.get("test.triangleIntermidiate");

	    for (int i = 0; i < p; ++i) {
		for (int j = 0; j < m; ++j) {
		    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(path + "-" + i + "-" + j), getConf());
		}
	    }

	    Job job3 = new Job(conf);

	    job3.setJarByClass(getClass());
	    job3.setJobName("Near5Clique");

	    job3.getConfiguration().setInt("mapred.line.input.format.linespermap",
		    seedListSize / 240 == 0 ? 1 : seedListSize / 240);

        FileInputFormat.setInputPaths(job3, new Path(seed));

	    job3.setInputFormatClass(NLineInputFormat.class);
	    job3.setOutputFormatClass(NullOutputFormat.class);

	    job3.setNumReduceTasks(0);

	    job3.setMapperClass(Near5CliqueNMapper3.class);

	    job3.waitForCompletion(true);
	} else {
	    String path = conf.get("test.triangleIntermidiate");

	    for (int i = 0; i < p; ++i) {
		for (int j = 0; j < m; ++j) {
		    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(path + "-" + i + "-" + j), getConf());
		}
	    }

	    Job job3 = new Job(conf);

	    job3.setJarByClass(getClass());
	    job3.setJobName("Near5CliqueO");

	    job3.getConfiguration().setInt("mapred.line.input.format.linespermap",
		    seedListSize / 240 == 0 ? 1 : seedListSize / 240);

        FileInputFormat.setInputPaths(job3, new Path(seed));

	    job3.setInputFormatClass(NLineInputFormat.class);
	    job3.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
//	    SequenceFileOutputFormat.setOutputCompressionType(job3, CompressionType.BLOCK);
//	    FileOutputFormat.setCompressOutput(job3, true);
//	    FileOutputFormat.setOutputCompressorClass(job3, SnappyCodec.class);

	    job3.setMapOutputKeyClass(ThreeNodeKey.class);
	    job3.setMapOutputValueClass(TwoIntDegreeArray.class);

	    job3.setOutputKeyClass(ThreeNodeKey.class);
	    job3.setOutputValueClass(TwoIntDegreeArray.class);

	    job3.setNumReduceTasks(0);

	    job3.setMapperClass(Near5CliqueNOMapper3.class);

	    job3.waitForCompletion(true);
	}

	FileSystem.get(conf).delete(new Path(args[0] + ".temp3"));

		FileSystem.get(conf).delete(new Path(args[0] + ".temp"));
		FileSystem.get(conf).delete(new Path(args[0] + ".temp2"));
		FileSystem.get(conf).delete(new Path(args[0] + ".seed"));
		FileSystem.get(conf).delete(new Path(args[0] + ".temp3"));

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
	ToolRunner.run(conf, new Near5CliqueO(), args);

	endTime = System.currentTimeMillis();
	log.info("[Near5Clique] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
