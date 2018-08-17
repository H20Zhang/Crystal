package sDeprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import sData.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class TestToWaitDelete {
    public static class ThreeTriangleMapper3 extends Mapper<Object, Text, NullWritable, NullWritable> {

	private final HashMap<TwoNodeKey, IntDegreeArray> triangleList = new HashMap<TwoNodeKey, IntDegreeArray>();
	private final OneNodeKey node1 = new OneNodeKey();
	private final OneNodeKey node2 = new OneNodeKey();
	private final OneNodeKey node3 = new OneNodeKey();

	private final TwoNodeKey leftEdge = new TwoNodeKey();
	private final TwoNodeKey rightEdge = new TwoNodeKey();

	private Integer p;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
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

	private void readTriangle(String path, Context context, int i, int j) throws IOException {
	    Configuration conf = context.getConfiguration();

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    // reader = new SequenceFile.Reader(fs, new Path(path), conf);
	    reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[i * (p / 4) + j], conf);
	    TwoNodeKey key = (TwoNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    IntDegreeArray value = (IntDegreeArray) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    long position = reader.getPosition();
	    while (reader.next(key, value)) {
		String syncSeen = reader.syncSeen() ? "*" : "";
		// System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
		// key, value);
		position = reader.getPosition(); // beginning of next record

		triangleList.put(WritableUtils.clone(key, context.getConfiguration()),
			WritableUtils.clone(value, context.getConfiguration()));

		// if ((Math.abs(value.key.hashCode()) % p) != i) {
		// context.getCounter("test","errorPartition1").increment(1);
		// }
		//
		// if (value.array != null) {
		// context.getCounter("test","notEmpty").increment(1);
		// }
	    }

	    IOUtils.closeStream(reader);
	}

	private void enumerateSubgraph(String path, Context context, int i, int j) throws IOException {
	    Configuration conf = context.getConfiguration();

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    reader = new SequenceFile.Reader(fs, new Path(path), conf);
	    PartitionTwoKey key = (PartitionTwoKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    TriangleValue value = (TriangleValue) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    long position = reader.getPosition();
	    while (reader.next(key, value)) {
		String syncSeen = reader.syncSeen() ? "*" : "";
		// System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
		// key, value);
		position = reader.getPosition(); // beginning of next record

		node1.set(value.node12.node1, value.node12.node1Degree);
		node2.set(value.node12.node2, value.node12.node2Degree);
		node3.set(value.node3.node, value.node3.degree);

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

		if ((Math.abs(leftEdge.hashCode()) % p) != i) {
		    context.getCounter("test", "errorPartition2").increment(1);
		}

		if ((Math.abs(rightEdge.hashCode()) % p) != j) {
		    context.getCounter("test", "errorPartition2").increment(1);
		}

		IntDegreeArray leftEdgeList = triangleList.get(leftEdge);
		IntDegreeArray rightEdgeList = triangleList.get(rightEdge);

		if (leftEdgeList == null && rightEdgeList == null) {
		    context.getCounter("test", "non").increment(1);
		}

		if (leftEdgeList == null && rightEdgeList != null) {
		    context.getCounter("test", "left").increment(1);
		}

		if (leftEdgeList != null && rightEdgeList == null) {
		    context.getCounter("test", "right").increment(1);
		}

		if (leftEdgeList != null && rightEdgeList != null) {
		    context.getCounter("test", "Result")
			    .increment((leftEdgeList.size - 1) * (rightEdgeList.size - 2) / 2);
		}

	    }

	    IOUtils.closeStream(reader);
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, NullWritable>.Context context)
		throws IOException, InterruptedException {
	    List<Integer> problems = getProblem(value);

	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate");

	    for (int i = 0; i < p / 4; ++i) {
		String inputPath = path + "-" + problems.get(0) + "-" + i;
		readTriangle(inputPath, context, problems.get(0), i);
	    }

	    for (int i = 0; i < p / 4; ++i) {
		String inputPath = path + "-" + problems.get(1) + "-" + i;
		readTriangle(inputPath, context, problems.get(1), i);
	    }

	    String path2 = conf.get("test.triangleIntermidiate2");
	    String inputPath2 = path2 + "-" + problems.get(0) + "-" + problems.get(1);
	    enumerateSubgraph(inputPath2, context, problems.get(0), problems.get(1));

	    triangleList.clear();
	}
    }

    public static class FourTriangleMapper3 extends Mapper<Object, Text, NullWritable, NullWritable> {

	private final HashMap<TwoNodeKey, IntDegreeArray> triangleList = new HashMap<TwoNodeKey, IntDegreeArray>();
	private final OneNodeKey node1 = new OneNodeKey();
	private final OneNodeKey node2 = new OneNodeKey();
	private final OneNodeKey node3 = new OneNodeKey();

	private final TwoNodeKey leftEdge = new TwoNodeKey();
	private final TwoNodeKey rightEdge = new TwoNodeKey();

	private Integer p;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
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

	private void readTriangle(String path, Context context, int i, int j) throws IOException {
	    Configuration conf = context.getConfiguration();

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    // reader = new SequenceFile.Reader(fs, new Path(path), conf);
	    reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[i * (p / 4) + j], conf);
	    TwoNodeKey key = (TwoNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    IntDegreeArray value = (IntDegreeArray) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    long position = reader.getPosition();
	    while (reader.next(key, value)) {
		String syncSeen = reader.syncSeen() ? "*" : "";
		// System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
		// key, value);
		position = reader.getPosition(); // beginning of next record

		triangleList.put(WritableUtils.clone(key, context.getConfiguration()),
			WritableUtils.clone(value, context.getConfiguration()));

		// if ((Math.abs(value.key.hashCode()) % p) != i) {
		// context.getCounter("test","errorPartition1").increment(1);
		// }
		//
		// if (value.array != null) {
		// context.getCounter("test","notEmpty").increment(1);
		// }
	    }

	    IOUtils.closeStream(reader);
	}

	private void enumerateSubgraph(String path, Context context, int i, int j) throws IOException {
	    Configuration conf = context.getConfiguration();

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    reader = new SequenceFile.Reader(fs, new Path(path), conf);
	    PartitionTwoKey key = (PartitionTwoKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    TriangleValue value = (TriangleValue) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    long position = reader.getPosition();
	    while (reader.next(key, value)) {
		String syncSeen = reader.syncSeen() ? "*" : "";
		// System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
		// key, value);
		position = reader.getPosition(); // beginning of next record

		node1.set(value.node12.node1, value.node12.node1Degree);
		node2.set(value.node12.node2, value.node12.node2Degree);
		node3.set(value.node3.node, value.node3.degree);

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

		if ((Math.abs(leftEdge.hashCode()) % p) != i) {
		    context.getCounter("test", "errorPartition2").increment(1);
		}

		if ((Math.abs(rightEdge.hashCode()) % p) != j) {
		    context.getCounter("test", "errorPartition2").increment(1);
		}

		IntDegreeArray leftEdgeList = triangleList.get(leftEdge);
		IntDegreeArray rightEdgeList = triangleList.get(rightEdge);

		if (leftEdgeList == null && rightEdgeList == null) {
		    context.getCounter("test", "non").increment(1);
		}

		if (leftEdgeList == null && rightEdgeList != null) {
		    context.getCounter("test", "left").increment(1);
		}

		if (leftEdgeList != null && rightEdgeList == null) {
		    context.getCounter("test", "right").increment(1);
		}

		if (leftEdgeList != null && rightEdgeList != null) {
		    context.getCounter("test", "Result")
			    .increment((leftEdgeList.size - 1) * (rightEdgeList.size - 2) / 2);
		}

	    }

	    IOUtils.closeStream(reader);
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, NullWritable>.Context context)
		throws IOException, InterruptedException {
	    List<Integer> problems = getProblem(value);

	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate");

	    for (int i = 0; i < p / 4; ++i) {
		String inputPath = path + "-" + problems.get(0) + "-" + i;
		readTriangle(inputPath, context, problems.get(0), i);
	    }

	    for (int i = 0; i < p / 4; ++i) {
		String inputPath = path + "-" + problems.get(1) + "-" + i;
		readTriangle(inputPath, context, problems.get(1), i);
	    }

	    String path2 = conf.get("test.triangleIntermidiate2");
	    String inputPath2 = path2 + "-" + problems.get(0) + "-" + problems.get(1);
	    enumerateSubgraph(inputPath2, context, problems.get(0), problems.get(1));

	    triangleList.clear();
	}
    }
}
