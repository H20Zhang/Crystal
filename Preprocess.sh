prefix="/user/hzhang/subgraph" #need to specify your own location
input=(eu-core) #need to specify your own input
declare -A edgeNumDic=(["as"]="11095298" ["wikiV"]="103689" ["eu-core"]="25571")
bloomHash="3"
bloomBit="6"

for i in ${input[@]}; do
	
	
	data=$i
	
	root="${prefix}/${data}"
	datalocation="${prefix}/Data/${data}" #datasets needs to be uploaded to this folder.
	edge=${edgeNumDic[$data]}
	
	hadoop fs -mkdir ${root}

	source="${root}/preprocess_${data}"
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r ${root}
	hadoop jar Subgraph.jar sPreprocess.sTool.PreprocessManager -D mapreduce.map.memory.mb=4000 -D mapreduce.reduce.memory.mb=4000 -D mapreduce.job.reduces=240 ${datalocation} ${source}
	hadoop jar Subgraph.jar sPreprocess.sTool.CountDegree -D mapreduce.map.memory.mb=4000 -D mapreduce.reduce.memory.mb=4000 -D mapreduce.job.reduces=240  $source ${degreeEdgeTemp} ${degreeEdge}
	hadoop jar Subgraph.jar sPreprocess.sTool.BloomFilterGeneratorManager $source ${prefix}/${data}/bloom_${data} $edge $bloomBit $bloomHash
	hadoop jar Subgraph.jar sPreprocess.sTool.PreprocessForNodePlusManager -D mapreduce.map.memory.mb=4000 -D mapreduce.reduce.memory.mb=4000 -D mapreduce.job.reduces=240  $degreeEdge $plusA
	hadoop jar Subgraph.jar sPreprocess.sTool.PreprocessPlusForFourCliqueManager -D mapreduce.map.memory.mb=4000 -D mapreduce.reduce.memory.mb=4000 -D mapreduce.job.reduces=240  $degreeEdge $plusB
	hadoop jar Subgraph.jar sPreprocess.sCliqueGeneration.CliqueGeneration -D mapreduce.map.memory.mb=4000 -D mapreduce.reduce.memory.mb=4000 -D mapreduce.job.reduces=240  $degreeEdge $plusA $outputA $plusB $outputB "" "" $bloom 2
	hadoop jar Subgraph.jar sPreprocess.sCliqueGeneration.TwoClique -D mapreduce.map.memory.mb=4000 -D mapreduce.reduce.memory.mb=4000 -D mapreduce.job.reduces=240  $degreeEdge $line
done


