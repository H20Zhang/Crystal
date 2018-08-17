package sAlg;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sData.*;
import sDeprecated.BowlO.OneNodeKeyIntDegreeArray;

import java.io.IOException;

/**
 * <h1>
 * Count Quad Triangle
 * </h1>
 * <ul>
 * 	<li>Pattern name: Quad Triangle </li>
 * 	<li>Pattern: (0,1),(0,2),(0,5),(1,2),(2,3),(2,4),(2,5),(3,4),(4,5)  </li>
 * 	<li>Core: (0,2,4)</li>
 * 	<li>Non Core Node: [1,3,5]</li>
 * </ul>
 * @author zhanghao
 */

public class QuadTriangle extends Configured implements Tool {

    private static Logger log = Logger.getLogger(QuadTriangle.class);

    public static class QuadTriangleCMapper
	    extends Mapper<TwoNodeKey, IntDegreeArray, OneNodeKey, OneNodeKeyIntDegreeArray> {

	private final OneNodeKey aKey = new OneNodeKey();
	private final OneNodeKeyIntDegreeArray aValue = new OneNodeKeyIntDegreeArray();

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {

		IntDegreeArray value1 = new IntDegreeArray(value.size);
		for (int i = 0; i < value.size; i++) {
			value1.addNodeAndDegree(i, value.getNode(i), value.getDegree(i));
		}

	    {


			aKey.set(key.node1, key.node1Degree);

		OneNodeKey tempKey = new OneNodeKey();
		tempKey.set(key.node2, key.node2Degree);

		TIntArrayList tempList = new TIntArrayList();
			for (int i = 0; i < value1.size; ++i) {
				OneNodeKey tempNode = new OneNodeKey();
				tempNode.node = value1.getNode(i);
				tempNode.degree = value1.getDegree(i);

		    // if (tempNode.node > tempKey.node){
		    tempList.add(tempNode.node);
		    // }
		}

		tempList.sort();

		IntDegreeArray outputValue = new IntDegreeArray(tempList.size());
		for (int i = 0; i < tempList.size(); ++i) {
		    outputValue.addNodeAndDegree(i, tempList.get(i), tempList.get(i));
		}

		aValue.set(tempKey, outputValue);

		context.write(aKey, aValue);
	    }

	    {
		aKey.set(key.node2, key.node2Degree);
		OneNodeKey tempKey1 = new OneNodeKey();
		tempKey1.set(key.node1, key.node1Degree);

		TIntArrayList tempList1 = new TIntArrayList();
			for (int i = 0; i < value1.size; ++i) {
				OneNodeKey tempNode = new OneNodeKey();
				tempNode.node = value1.getNode(i);
				tempNode.degree = value1.getDegree(i);

		    // if (tempNode.node > tempKey1.node) {
		    tempList1.add(tempNode.node);
		    // }
		}

		tempList1.sort();
		IntDegreeArray outputValue1 = new IntDegreeArray(tempList1.size());
		for (int i = 0; i < tempList1.size(); ++i) {
		    outputValue1.addNodeAndDegree(i, tempList1.get(i), tempList1.get(i));
		}

		aValue.set(tempKey1, outputValue1);

		context.write(aKey, aValue);
	    }
	}
    }

    public static class QuadTriangleCReducer
	    extends Reducer<OneNodeKey, OneNodeKeyIntDegreeArray, NullWritable, NullWritable> {
	private long intersect(TIntArrayList uN, TIntArrayList vN) throws IOException, InterruptedException {
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

		    count += 1L;
		    uCur++;
		    vCur++;
		}
	    }

	    return count;
	}

		private TIntArrayList intersectEnumerate(TIntArrayList uN, TIntArrayList vN) throws IOException, InterruptedException {

		TIntArrayList theList = new TIntArrayList();
			if ((uN == null) || (vN == null)) {
				return theList;
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

					count += 1L;
					theList.add(vN.getQuick(vCur));
					uCur++;
					vCur++;

				}
			}

			return theList;
		}

		long Counter = 0;

		public void enumerate(int node0, int node1, int node2, int node3, int node4, int node5, Context context){
			++Counter;
//			context.getCounter("test","enumeration").increment(1);
		}

	@Override
	protected void reduce(OneNodeKey key, Iterable<OneNodeKeyIntDegreeArray> values, Context context)
		throws IOException, InterruptedException {

	    TIntArrayList tempList = new TIntArrayList();

	    TIntObjectHashMap<TIntArrayList> theMap = new TIntObjectHashMap<TIntArrayList>();
	    for (OneNodeKeyIntDegreeArray temp : values) {
		TIntArrayList tempArray = new TIntArrayList(temp.array.nodeArray);

		theMap.put(temp.key.node, tempArray);
		tempList.add(temp.key.node);
	    }

	    tempList.sort();

	    for (int i = 0; i < tempList.size(); ++i) {
		for (int j = i + 1; j < tempList.size(); ++j) {
			int leftNode = tempList.get(j);
			int rightNode = tempList.get(i);

			TIntArrayList leftList = theMap.get(leftNode);
			TIntArrayList rightList = theMap.get(rightNode);

		    long leftCount = leftList.size();
		    long rightCount = rightList.size();


			Boolean isEnumerating = context.getConfiguration().getBoolean("test.isEmitted", false);



			//counting
			TIntArrayList rightListCopy = new TIntArrayList(rightList.size());
			TIntArrayList leftListCopy = new TIntArrayList(leftList.size());


			for (int k = 0; k < rightList.size(); k++) {
				rightListCopy.add(rightList.getQuick(k));
			}

			for (int k = 0; k < leftList.size(); k++) {
				leftListCopy.add(leftList.getQuick(k));
			}

			rightListCopy.remove(leftNode);
			leftListCopy.remove(rightNode);

			long leftCountCopy = leftListCopy.size();
			long rightCountCopy = rightListCopy.size();

			long counter_intersection_copy = intersect(leftListCopy, rightListCopy);

			long counterCopy = leftCountCopy * rightCountCopy * counter_intersection_copy - counter_intersection_copy * (leftCountCopy + rightCountCopy - 2) - counter_intersection_copy * counter_intersection_copy;

			context.getCounter("test", "counting1.5").increment(counterCopy);


			//enumerating
			TIntArrayList list2 = intersectEnumerate(leftList,rightList);
			for (int k = 0; k < list2.size(); k++) {


				if (isEnumerating) {
					//enumerating
					for (int l = 0; l < leftList.size(); l++) {
						for (int m = 0; m < rightList.size(); m++) {
							int node4 = key.node;
							int node2 = leftNode;
							int node0 = rightNode;
							int node1 = list2.get(k);
							int node3 = leftList.get(l);
							int node5 = rightList.get(m);

							if (node3 != node1 && node3 != node0 && node5 != node1 && node5 != node2 && node5 != node3) {
								enumerate(node0, node1, node2, node3, node4, node5, context);
							}
						}
					}
				}


			}

			//enumerating
//			TIntArrayList list2 = intersectEnumerate(leftList,rightList);

//
//			for (int k = 0; k < list2.size(); k++) {
//				for (int l = 0; l < leftList.size(); l++) {
//					for (int m = 0; m < rightList.size(); m++) {
//						int node4 = key.node;
//						int node2 = leftNode;
//						int node0 = rightNode;
//						int node1 = list2.get(k);
//						int node3 = leftList.get(l);
//						int node5 = rightList.get(m);
//
//						if (node3 != node1 && node3 != node0 && node3 != node5 && node5 != node1 && node5 != node2 && node5 != node3){
//							enumerate(node0,node1,node2,node3,node4,node5,context);
//						}
//					}
//				}
//			}



		}
	    }

	}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			context.getCounter("test","enumeration").increment(Counter);
		}
	}

    public static class QuadTriangleCOReducer
	    extends Reducer<OneNodeKey, OneNodeKeyIntDegreeArray, ThreeNodeKey, TwoIntDegreeArray> {
	@Override
	protected void reduce(OneNodeKey key, Iterable<OneNodeKeyIntDegreeArray> values, Context context)
		throws IOException, InterruptedException {

	    TIntArrayList tempList = new TIntArrayList();

	    TIntObjectHashMap<TIntArrayList> theMap = new TIntObjectHashMap<TIntArrayList>();
	    for (OneNodeKeyIntDegreeArray temp : values) {
		theMap.put(temp.key.node, new TIntArrayList(temp.array.nodeArray));
		tempList.add(temp.key.node);
	    }

	    TIntObjectIterator<TIntArrayList> iterator = theMap.iterator();

	    // context.getCounter("test", "test").increment(1);

	    ThreeNodeKey outputKey = new ThreeNodeKey();
	    TwoIntDegreeArray outputValue = new TwoIntDegreeArray();

	    tempList.sort();

	    if (tempList.size() >= 2) {
		for (int i = 0; i < tempList.size(); ++i) {
		    for (int j = i + 1; j < tempList.size(); ++j) {
			TIntArrayList left = theMap.get(tempList.get(i));
			TIntArrayList right = theMap.get(tempList.get(j));

			IntDegreeArray array1 = new IntDegreeArray(left.size());
			IntDegreeArray array2 = new IntDegreeArray(right.size());

			array1.nodeArray = left.toArray();
			array1.degreeArray = left.toArray();

			array2.nodeArray = right.toArray();
			array2.degreeArray = right.toArray();

			outputValue.array1 = array1;
			outputValue.array2 = array2;

			outputKey.set(key.node, tempList.get(i), tempList.get(j), key.degree, tempList.get(i),
				tempList.get(j));

			context.write(outputKey, outputValue);
		    }
		}
	    }

	}
    }

    @Override
    public int run(String[] args) throws Exception {

	Configuration conf = getConf();

	Boolean isOutput = conf.getBoolean("test.isOutput", false);
	int memory_size = conf.getInt("test.memory", 4000);
	int opts_size = (int) (memory_size * 0.85);
	String memory_opts = "-Xmx" + opts_size + "M";

	conf.setInt("mapreduce.map.memory.mb", memory_size);
	conf.set("mapreduce.map.java.opts", memory_opts);
	conf.setInt("mapreduce.reduce.memory.mb", memory_size);
	conf.set("mapreduce.reduce.java.opts", memory_opts);

	if (isOutput == false) {
	    Job job = new Job(conf);

	    job.setJarByClass(getClass());
	    job.setJobName("QuadTriangle");
	    job.setNumReduceTasks(240);

	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(NullOutputFormat.class);

	    FileInputFormat.setInputPaths(job, new Path(args[0]));

	    job.setMapperClass(QuadTriangleCMapper.class);
	    job.setReducerClass(QuadTriangleCReducer.class);

	    job.setMapOutputKeyClass(OneNodeKey.class);
	    job.setMapOutputValueClass(OneNodeKeyIntDegreeArray.class);

	    boolean success = job.waitForCompletion(true);
	} else {
	    Job job = new Job(conf);

	    job.setJarByClass(getClass());
	    job.setJobName("QuadTriangle");
		job.setNumReduceTasks(240);

	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

//	    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//	    FileOutputFormat.setCompressOutput(job, true);
//	    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

	    job.setMapperClass(QuadTriangleCMapper.class);
	    job.setReducerClass(QuadTriangleCOReducer.class);

	    job.setOutputKeyClass(ThreeNodeKey.class);
	    job.setOutputValueClass(TwoIntDegreeArray.class);

	    job.setMapOutputKeyClass(OneNodeKey.class);
	    job.setMapOutputValueClass(OneNodeKeyIntDegreeArray.class);

	    boolean success = job.waitForCompletion(true);

	    
	}
	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	long milliSeconds = 10000 * 60 * 60;
	conf.setLong("mapred.task.timeout", milliSeconds);
	ToolRunner.run(conf, new QuadTriangle(), args);
	endTime = System.currentTimeMillis();
	log.info("[QuadTriangle] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
