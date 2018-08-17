

input=(wb as soclj gplus uk)
declare -A edgeNumDic=( ["wb"]="7600595" ["as"]="11095298" ["ork"]="117185083" ["lj"]="34681189" ["soclj"]="43681189"  ["gplus"]="13673453" ["uk"]="298113762" ["fd"]="1806067135")

initRun(){
	root="/user/tri/subgraph"
	data=$1
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"
}

initPreprocess(){
	root="/user/tri/subgraph"
	bloomHash="3"
	bloomBit="6"
	data=$1
	edge=${edgeNumDic[$data]}
	
	source="/user/tri/preprocess_${data}"

	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"
}

#preprocessing

input=(tw)
declare -A edgeNumDic=( ["wb"]="7600595" ["as"]="11095298" ["ork"]="117185083" ["lj"]="34681189" ["soclj"]="43681189"  ["gplus"]="13673453" ["uk"]="298113762" ["fd"]="1806067135" ["tw"]="1456067135")

for i in ${input[@]}; do
	root="/user/tri/subgraph"
	bloomHash="3"
	bloomBit="6"
	data=$i
	edge=${edgeNumDic[$data]}
	
	source="/user/tri/preprocess_${data}"

	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop jar Subgraph.jar sTool.PreprocessManager /user/tri/source_graph/source_$data ${source}
	hadoop jar Subgraph.jar sTool.CountDegree $source ${degreeEdgeTemp} ${degreeEdge}
	hadoop jar Subgraph.jar sTool.BloomFilterGeneratorManager $source ${root}/bloom_${data} $edge $bloomBit $bloomHash
	hadoop jar Subgraph.jar sTool.PreprocessForNodePlusManager $degreeEdge $plusA
	hadoop jar Subgraph.jar sTool.PreprocessPlusForFourCliqueManager $degreeEdge $plusB
	hadoop jar Subgraph.jar CliqueGeneration.CliqueGeneration $degreeEdge $plusA $outputA $plusB $outputB $bloom
	hadoop jar Subgraph.jar CliqueGeneration.TwoClique $degreeEdge $line
done

Running time
declare -A partition_size=(["wb"]="1" ["as"]="1" ["soclj"]="1" ["gplus"]="1" ["uk"]="4")
jobs=(fd)
for i in ${jobs[@]}; do

	#init variable
	initRun $i

	#whether use statistic mode
	statistic=false

	#whether output
	output=true

	if [[ $statistic == true ]];  then
		st="O"
		echo $st
	else
		st=""
		echo $st
	fi

	if [[ $output == true ]];  then
		st="O"
		out="true"
		echo $st
	else
		st=""
		out="false"
		echo $st
	fi

	#parition_size
	p=${partition_size[$data]}
	p=1

	hadoop jar Subgraph.jar TestMerge.ChordalSquare$st -D test.isOutput=$out $outputA
	hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus$st -D test.isOutput=$out $outputA
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof$st -D test.isOutput=$out -D test.p=$p $outputA $line
	hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus$st -D test.isOutput=$out -D test.p=$p $outputA $line
	hadoop jar Subgraph.jar TestIntersection.Square$st -D test.isOutput=$out -D test.p=$p $line $line
	hadoop jar Subgraph.jar TestIntersection.House$st -D test.isOutput=$out -D test.p=$p $outputA $line
	hadoop jar Subgraph.jar TestIntersection.SolarSquare$st -D test.isOutput=$out -D test.p=$p $outputA $line

	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM$st -D test.isOutput=$out -D test.p=$p $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle$st -D test.isOutput=$out -D test.p=`expr $p \* 40` $outputA
done





#Statisctic





# #preprocessing





#calculate all the jobs

jobs=(as)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	 #hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	 #hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	 #hadoop jar Subgraph.jar TestIntersection.Square $line $line
	# hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
	# hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done

jobs=(as soclj ork gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
done



# hadoop jar Subgraph.jar TestIntersection.TestMultiRoundStrategy  /user/tri1/subgraph/tri_fd1 /user/tri1/subgraph/line_fd





# hadoop fs -rm -r /user/tri1/subgraph/tri_fd1.seed
# hadoop fs -rm -r /user/tri1/subgraph/tri_fd1.temp
# hadoop fs -rm -r /user/tri1/subgraph/tri_fd1.triangleIntermidiate

# hadoop fs -rm -r /user/tri1/subgraph/tri_ork.temp
# hadoop fs -rm -r /user/tri1/subgraph/tri_ork.temp2
# hadoop fs -rm -r /user/tri1/subgraph/tri_ork.seed
# hadoop fs -rm -r /user/tri1/subgraph/tri_ork.triangleIntermidiate
# hadoop fs -rm -r /user/tri1/subgraph/tri_ork.triangleIntermidiate2


jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	# hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	# hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	# hadoop jar Subgraph.jar TestIntersection.Square $line $line
	# hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	# hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
	


	hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=1 -D test.memory=3000 $outputA $line 1
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=1 -D test.memory=2500 $outputA $line 1
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=1 -D test.memory=2000 $outputA $line 1
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=2 -D test.memory=1500 $outputA $line 2
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=3 -D test.memory=1000 $outputA $line 3

	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -D test.p=30 -D test.memory=3000 $outputA 
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -D test.p=40 -D test.memory=2500 $outputA 
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -D test.p=50 -D test.memory=2000 $outputA 
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -D test.p=60 -D test.memory=1500 $outputA
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -D test.p=70 -D test.memory=1000 $outputA  

	hadoop jar Subgraph.jar TestMerge.ChordalSquare -D test.memory=3000 $outputA
	hadoop jar Subgraph.jar TestMerge.ChordalSquare -D test.memory=2500 $outputA
	hadoop jar Subgraph.jar TestMerge.ChordalSquare -D test.memory=2000 $outputA
	hadoop jar Subgraph.jar TestMerge.ChordalSquare -D test.memory=1500 $outputA
	hadoop jar Subgraph.jar TestMerge.ChordalSquare -D test.memory=1000 $outputA
done


jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	# hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	# hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	# hadoop jar Subgraph.jar TestIntersection.Square $line $line
	# hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	# hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
	

	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=1 -D test.memory=3000 $outputA $line 1
	# hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=2 -D test.memory=3000 $outputA $line 2
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=3 -D test.memory=4000 $outputA $line 3
	# hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=4 -D test.memory=2000 $outputA $line 4
	# hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=5 -D test.memory=1500 $outputA $line 5

	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -D test.p=70 -D test.memory=4000 $outputA 
	

	hadoop jar Subgraph.jar TestMerge.ChordalSquare -D test.memory=4000 $outputA
done


#output result

jobs=(as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangleO $outputA

	#hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line /user/tri/temp1
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=3 -D test.memory=4000 $outputA $line 3
	# hadoop fs -rm -r /user/tri/temp1
	hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlusO -D test.p=3 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestMerge.ChordalSquarePlusO $outputA
	hadoop jar Subgraph.jar TestMerge.ChordalSquareO $outputA
done



jobs=(as)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop jar Subgraph.jar TestPartiton.BowlO -D test.p=40 -D test.memory=4000 $line


done



# output size for twigtwin remainging experiment
input=(soclj)
querys=(three_triangle solarsquare)
for j in ${input[@]}; do
	for i in ${querys[@]}; do
		jarFile=Twig.jar
		inputFile="/user/tri/twig_${j}/source_${j}"
		query=$i
		numReducers=240
		cliqueNodes=0

		hadoop jar $jarFile dbg.hadoop.subgenum.twintwig.MainEntry \
			mapred.input.file=$inputFile \
			mapred.reduce.tasks=$numReducers \
			jar.file.name=$jarFile \
			enum.query=$query \
			count.only="false"\
			clique.number.vertices=$cliqueNodes
	done
done


#vary openess (triangle vs wedge, bowl vs square, all datasets, no output)

echo "vary openess"

jobs=(ork)
for i in ${jobs[@]}; do
	echo "vary openess for ${i}"
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	#remeber triangle is outputed three times more
	hadoop jar Subgraph.jar TestMerge.TriangleO $outputA
	hadoop jar Subgraph.jar TestMerge.WedgeO -D test.wedge_num=2 $line
	hadoop jar Subgraph.jar TestPartiton.BowlO -D test.p=40 -D test.memory=4000 $line

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.SquareO $line $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done

# jobs=(uk)
# for i in ${jobs[@]}; do
# 	echo "vary openess for ${i}"
# 	root="/user/tri/subgraph"
# 	data=$i
# 	degreeEdgeTemp="${root}/countDegreeA_${data}"
# 	degreeEdge="${root}/countDegreeB_${data}"
# 	outputA="${root}/tri_${data}"
# 	outputB="${root}/four_${data}"
# 	bloom="${root}/bloom_${data}/bloom_file/bloom"
# 	plusA="${root}/preprocessPlus_${data}"
# 	plusB="${root}/preprocessFourPlus_${data}"
# 	line="${root}/line_${data}"


# 	#remeber triangle is outputed three times more
# 	hadoop jar Subgraph.jar TestMerge.TriangleO $outputA
# 	hadoop jar Subgraph.jar TestMerge.WedgeO -D test.wedge_num=2 $line
# 	hadoop jar Subgraph.jar TestPartiton.BowlO -D test.p=100 -D test.memory=4000 $line

# 	hadoop fs -rm -r /user/tri/temp1_${i}
# 	hadoop jar Subgraph.jar TestIntersection.SquareO -D test.p=4 $line $line /user/tri/temp1_${i}
# 	hadoop fs -rm -r /user/tri/temp1_${i}
# done


#vary free node (four star, chordal square plus, solar square pedge, use livejournal datasets, no output)
echo "vary free node"
jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop jar Subgraph.jar TestMerge.WedgeO -D test.wedge_num=4 $line

	hadoop jar Subgraph.jar TestMerge.ChordalSquarePlusO $outputA

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquareO $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop jar Subgraph.jar TestMerge.WedgeO -D test.wedge_num=4 $line

	hadoop jar Subgraph.jar TestMerge.ChordalSquarePlusO $outputA

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquareO -D test.p=4 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done

#vary datasets compression rate (chordal roof, all datasets, no output)
echo "vary compression rate"
jobs=(wb as gplus uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestMerge.ChordalSquareO -D test.p=1 -D test.memory=4000 $outputA /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done

# jobs=(uk)
# for i in ${jobs[@]}; do
# 	root="/user/tri/subgraph"
# 	data=$i
# 	degreeEdgeTemp="${root}/countDegreeA_${data}"
# 	degreeEdge="${root}/countDegreeB_${data}"
# 	outputA="${root}/tri_${data}"
# 	outputB="${root}/four_${data}"
# 	bloom="${root}/bloom_${data}/bloom_file/bloom"
# 	plusA="${root}/preprocessPlus_${data}"
# 	plusB="${root}/preprocessFourPlus_${data}"
# 	line="${root}/line_${data}"

# 	hadoop fs -rm -r /user/tri/temp1_${i}
# 	hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.p=4 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
# 	hadoop fs -rm -r /user/tri/temp1_${i}
# done

#output time vary datasets (chordal roof, all datasets, output and computation time)
echo "vary output datasets time"
jobs=(ork)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done

jobs=(gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.isOutput=true -D test.p=3 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.isOutput=true -D test.p=4 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done

#output time vary pattern (livejournal, all patterns, output and computation time)
echo "output time vary pattern"
jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlusO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestIntersection.HouseO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestIntersection.SolarSquareO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestIntersection.SquareO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $line $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done

#new pattern time

# echo "new pattern"

# jobs=(wb)
# for i in ${jobs[@]}; do
# 	root="/user/tri/subgraph"
# 	data=$i
# 	degreeEdgeTemp="${root}/countDegreeA_${data}"
# 	degreeEdge="${root}/countDegreeB_${data}"
# 	outputA="${root}/tri_${data}"
# 	outputB="${root}/four_${data}"
# 	bloom="${root}/bloom_${data}/bloom_file/bloom"
# 	plusA="${root}/preprocessPlus_${data}"
# 	plusB="${root}/preprocessFourPlus_${data}"
# 	line="${root}/line_${data}"


# 	hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
# 	hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
# 	hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
# 	hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
# 	hadoop jar Subgraph.jar TestIntersection.Square $line $line
# 	hadoop jar Subgraph.jar TestIntersection.House $outputA $line
# 	hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
# 	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlus $outputA $line
# 	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
# done

jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	#hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	#hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.Square $line $line
	##hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlus $outputA $line
	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done


# jobs=(uk)
# for i in ${jobs[@]}; do
# 	root="/user/tri/subgraph"
# 	data=$i
# 	degreeEdgeTemp="${root}/countDegreeA_${data}"
# 	degreeEdge="${root}/countDegreeB_${data}"
# 	outputA="${root}/tri_${data}"
# 	outputB="${root}/four_${data}"
# 	bloom="${root}/bloom_${data}/bloom_file/bloom"
# 	plusA="${root}/preprocessPlus_${data}"
# 	plusB="${root}/preprocessFourPlus_${data}"
# 	line="${root}/line_${data}"

# 	hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA	
# 	hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus  -D test.p=4 -D test.memory=4000 $outputA $line
# done

jobs=()
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	#hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	#hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.Square -D test.p=2 $line $line
	##hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	hadoop jar Subgraph.jar TestIntersection.SolarSquare -D test.p=2 $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.p=4 $line $outputA
	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done

jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	#hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	#hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.Square -D test.p=1 $line $line
	##hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.SolarSquare -D test.p=1 $outputA $line
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=false -D test.p=4 $line $outputA temp_${i}
	hadoop fs -rm -r temp_${i}
	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done





# solar square plus O










jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	#hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	#hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.Square -D test.p=1 $line $line
	##hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.SolarSquare -D test.p=1 $outputA $line
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=false -D test.p=1 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	#hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	#hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.Square -D test.p=1 $line $line
	##hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.SolarSquare -D test.p=1 $outputA $line
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=false -D test.p=4 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done


jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	hadoop jar Subgraph.jar TestIntersection.Square -D test.p=1 $line $line
	#hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	hadoop jar Subgraph.jar TestIntersection.SolarSquare -D test.p=1 $outputA $line
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=true -D test.p=3 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	#hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	#hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.Square -D test.p=1 $line $line
	##hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.SolarSquare -D test.p=1 $outputA $line
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=true -D test.p=4 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done

echo "output time vary pattern"
jobs=(as wb)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=true -D test.p=1 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=true -D test.p=50 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
done




jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	#hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	#hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.Square -D test.p=1 $line $line
	##hadoop jar Subgraph.jar TestIntersection.House $outputA $line
	#hadoop jar Subgraph.jar TestIntersection.SolarSquare -D test.p=1 $outputA $line
	# hadoop fs -rm -r temp_${i}
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.memory=4000  -D test.isOutput=false -D test.p=4 $line $outputA temp2_${i}
	# hadoop fs -rm -r temp_${i}

	# hadoop fs -rm -r temp_${i}
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.memory=3500  -D test.isOutput=false -D test.p=6 $line $outputA temp2_${i}
	# hadoop fs -rm -r temp_${i}

	# hadoop fs -rm -r temp_${i}
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.memory=3000  -D test.isOutput=false -D test.p=8 $line $outputA temp2_${i}
	# hadoop fs -rm -r temp_${i}

	# hadoop fs -rm -r temp_${i}
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.memory=2500  -D test.isOutput=false -D test.p=10 $line $outputA temp2_${i}
	# hadoop fs -rm -r temp_${i}

	# hadoop fs -rm -r temp_${i}
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.memory=2000  -D test.isOutput=false -D test.p=15 $line $outputA temp2_${i}
	# hadoop fs -rm -r temp_${i}

	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.memory=1000  -D test.isOutput=false -D test.p=50 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
	#hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA
done


for (( i = 0; i < 30; i++ )); do
	mapred job -kill job_1474269561279_$((1001+i))
	sleep 1
done


jobs=(wb soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"



	# hadoop fs -rm -r temp_${i}
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.memory=4000  -D test.isOutput=false -D test.p=1 $line $outputA temp2_${i}
	# hadoop fs -rm -r temp_${i}

	hadoop jar Subgraph.jar TestIntersection.House $outputA $line
done


jobs=(wb as soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA	
done


echo "vary free node"
jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop jar Subgraph.jar TestMerge.WedgeO -D test.wedge_num=4 $line

	hadoop jar Subgraph.jar TestMerge.ChordalSquarePlusO $outputA

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquareO $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
done



# 10-30



jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquareO -D test.p=1 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=false -D test.p=1 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
done

jobs=(gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle $outputA	
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	
	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquareO -D test.p=4 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=false -D test.p=4 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.HouseM -D test.memory=4000  -D test.isOutput=false -D test.p=4 $line $outputA temp2_${i}
done


jobs=(tw)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	# hadoop jar Subgraph.jar TestMerge.ChordalSquarePlus $outputA
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof -D test.p=100 $outputA $line
	# hadoop jar Subgraph.jar TestIntersection.ChordalRoofPlus $outputA $line
	hadoop jar Subgraph.jar TestIntersection.Square -D test.p=100 $line $line
	hadoop fs -rm -r temp2_${i}
	hadoop jar Subgraph.jar TestIntersection.HouseM -D test.memory=4000  -D test.isOutput=false -D test.p=100 $line $outputA temp2_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquare -D test.p=100 $outputA $line
	# hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM -D test.isOutput=true -D test.p=100 $line $outputA temp2_${i}
	# hadoop fs -rm -r temp_${i}

	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=false -D test.p=100 $line $outputA temp2_${i}
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -D test.p=300 $outputA
done



echo "output time vary pattern"
jobs=(scolj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"



	hadoop fs -rm -r /user/tri/temp1_${i}

	 hadoop jar Subgraph.jar TestIntersection.SquareO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $line $line /user/tri/temp1_${i}
	 hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestMerge.ChordalSquareO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA /user/tri/temp1_${i}

	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.HouseO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}


	# hadoop jar Subgraph.jar TestPartiton.ThreeTriangleO -D test.isOutput=true $outputA /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.SolarSquareO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}
	
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=true -D test.p=1 $line $outputA  /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}
done








jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop fs -rm -r temp_${i}
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.memory=4000  -D test.isOutput=true -D test.p=4 $line $outputA temp2_${i}
	hadoop fs -rm -r temp_${i}

	# hadoop jar Subgraph.jar TestIntersection.House $outputA $line
done



#Three Clique
jobs=(wb as soclj gplus uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.ThreeClique -Dtest.p=20 $line
done

#Four Clique
jobs=(wb as soclj gplus)
jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FourCliquei -Dtest.p=20 $outputA	
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FourCliquei -Dtest.p=50 $outputA	
done

#Five Clique

jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FiveClique -Dtest.p=20 $outputA $outputB	
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FiveClique -Dtest.p=50 $outputA $outputB	
done

#Near5Clique

jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.Near5CliqueN -Dtest.p=20 $outputA	
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.Near5CliqueN -Dtest.p=50 $outputA	
done

#TwinTriangle
jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.TwinTriangleN  $line $outputA
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.TwinTriangleN -D test.p=80 $line $outputA
done

#output LJ

jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.ThreeClique -D test.isOutput=true -Dtest.p=20 $line
done

#Four Clique
jobs=(soclj)
jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FourClique -D test.isOutput=true -Dtest.p=20 $outputA	
done

#Five Clique

jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FiveClique -D test.isOutput=true -Dtest.p=20 $outputA $outputB	
done


#Near5Clique

jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.Near5CliqueN -D test.isOutput=true -Dtest.p=20 $outputA	
done

#TwinTriangle
jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.TwinTriangleN  -D test.isOutput=true $line $outputA
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r ${outputA}.temp
	hadoop jar Subgraph.jar TestMerge.TwinTriangleC  $outputA
done





jobs=(wb as soclj gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.ThreeClique -D test.isOutput=true -Dtest.p=20 $line
done

#Four Clique
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FourClique -D test.isOutput=true -Dtest.p=20 $outputA	
done

#Five Clique
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FiveClique -D test.isOutput=true -Dtest.p=20 $outputA $outputB	
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.ThreeClique -D test.isOutput=true -Dtest.p=50 $line
done

#Four Clique
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FourClique -D test.isOutput=true -Dtest.p=50 $outputA	
done

#Five Clique
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestPartiton.FiveClique -D test.isOutput=true -Dtest.p=50 $outputA $outputB	
done




jobs=(wb as soclj gplus uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r ${outputA}.temp
	hadoop jar Subgraph.jar TestMerge.QuadTriangleC  $outputA
done

# jobs=(soclj)
# for i in ${jobs[@]}; do
# 	root="/user/tri/subgraph"
# 	data=$i
# 	degreeEdgeTemp="${root}/countDegreeA_${data}"
# 	degreeEdge="${root}/countDegreeB_${data}"
# 	outputA="${root}/tri_${data}"
# 	outputB="${root}/four_${data}"
# 	bloom="${root}/bloom_${data}/bloom_file/bloom"
# 	plusA="${root}/preprocessPlus_${data}"
# 	plusB="${root}/preprocessFourPlus_${data}"
# 	line="${root}/line_${data}"

# 	hadoop fs -rm -r ${outputA}.temp
# 	hadoop jar Subgraph.jar TestMerge.QuadTriangleC -D test.isOutput=true $outputA
# done


#single machine
jobs=(wb)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	# conf.set("mapreduce.local.map.tasks.maximum", "12");
	# conf.set("mapreduce.local.reduce.tasks.maximum", "12");
	# conf.set("mapred.job.tracker", "local");
	# conf.set("mapreduce.framework.name", "local");
	# conf.setInt("mapreduce.map.memory.mb", memory_size);
	# conf.set("mapreduce.map.java.opts", memory_opts);
	# conf.setInt("mapreduce.reduce.memory.mb", memory_size);
	# conf.set("mapreduce.reduce.java.opts", memory_opts);

	hadoop jar Subgraph.jar TestIntersection.Square  $line $line
	# hadoop jar Subgraph.jar TestIntersection.Square -Dmapreduce -Dmapreduce.local.map.tasks.maximum=12 -Dmapreduce.local.reduce.tasks.maximum=12 -Dmapreduce.framework.name=local $line $line
	# hadoop jar Subgraph.jar TestMerge.ChordalSquare -Dmapreduce.local.map.tasks.maximum=12 -Dmapreduce.local.reduce.tasks.maximum=12 -Dmapreduce.framework.name=local  $outputA
	# hadoop jar Subgraph.jar TestIntersection.HouseM $line $outputA 
	# hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	# hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -Dtest.p=20 $outputA
	# hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
	# hadoop jar Subgraph.jar TestPartiton.Near5CliqueN -Dtest.isOutput=false -Dtest.p=20 $outputA
	# hadoop jar Subgraph.jar TestMerge.QuadTriangleC $outputA
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM  $line $outputA
done



jobs=(wb as soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestIntersection.Square  $line $line
	hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	hadoop jar Subgraph.jar TestIntersection.HouseM $line $outputA 
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -Dtest.p=20 $outputA
	hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
	hadoop jar Subgraph.jar TestPartiton.Near5CliqueN -Dtest.isOutput=false -Dtest.p=20 $outputA
	hadoop jar Subgraph.jar TestMerge.QuadTriangleC $outputA
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM  $line $outputA
done

jobs=(gplus)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestIntersection.Square  $line $line
	hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -Dtest.p=20 $outputA
	hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
	hadoop jar Subgraph.jar TestPartiton.Near5CliqueN -Dtest.isOutput=false -Dtest.p=20 $outputA
	hadoop jar Subgraph.jar TestMerge.QuadTriangleC $outputA
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM  $line $outputA
done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop jar Subgraph.jar TestIntersection.Square -Dtest.p=4  $line $line
	hadoop jar Subgraph.jar TestMerge.ChordalSquare $outputA
	# hadoop jar Subgraph.jar TestIntersection.ChordalRoof $outputA $line
	# hadoop jar Subgraph.jar TestPartiton.ThreeTriangle -Dtest.p=20 $outputA
	# hadoop jar Subgraph.jar TestIntersection.SolarSquare $outputA $line
	hadoop jar Subgraph.jar TestPartiton.Near5CliqueN -Dtest.isOutput=false -Dtest.p=4 $outputA
	# hadoop jar Subgraph.jar TestMerge.QuadTriangleC $outputA
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusM  $line $outputA
done

#compression rate
jobs=(wb as soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"


	hadoop jar Subgraph.jar TestPartiton.Near5CliqueNO -Dtest.isOutput=false -Dtest.p=20 $outputA
	hadoop jar Subgraph.jar TestMerge.QuadTriangleCO $outputA
	# hadoop fs -rm -r /user/tri/temp1_${i}
	# hadoop jar Subgraph.jar TestIntersection.SquareO -D test.isOutput=false -D test.p=1 -D test.memory=4000 $line $line /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestMerge.ChordalSquareO -D test.isOutput=false -D test.p=1 -D test.memory=4000 $outputA /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.HouseMO -D test.isOutput=false -D test.p=1 -D test.memory=4000 $line $outputA /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.isOutput=false -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}


	# hadoop jar Subgraph.jar TestPartiton.ThreeTriangleO -D test.isOutput=false $outputA /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.SolarSquareO -D test.isOutput=false -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}
	
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=false -D test.p=1 $line $outputA  /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}


done

jobs=(uk)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"



	# hadoop fs -rm -r /user/tri/temp1_${i}
	# hadoop jar Subgraph.jar TestIntersection.SquareO -D test.isOutput=false -D test.p=4 -D test.memory=4000 $line $line /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestMerge.ChordalSquareO -D test.isOutput=false -D test.p=4 -D test.memory=4000 $outputA /user/tri/temp1_${i}

	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.HouseMO -D test.isOutput=false -D test.p=4 -D test.memory=4000 $line $outputA /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.isOutput=false -D test.p=4 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestPartiton.ThreeTriangleO -D test.isOutput=false -D test.p=100 $outputA /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	# hadoop jar Subgraph.jar TestIntersection.SolarSquareO -D test.isOutput=false -D test.p=4 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestPartiton.Near5CliqueNO -D test.isOutput=false -Dtest.p=100 $outputA
	# hadoop jar Subgraph.jar TestMerge.QuadTriangleCO $outputA
	
	# hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=false -D test.p=4 $line $outputA  /user/tri/temp1_${i}
	# hadoop fs -rm -r /user/tri/temp1_${i}
done









#write and read
jobs=(soclj)
for i in ${jobs[@]}; do
	root="/user/tri/subgraph"
	data=$i
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestPartiton.Near5CliqueN -Dtest.isOutput=true -Dtest.p=20 $outputA /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestMerge.QuadTriangleC -Dtest.isOutput=true $outputA /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}

	hadoop fs -rm -r /user/tri/temp1_${i}
	hadoop jar Subgraph.jar TestIntersection.SquareO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $line $line /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestMerge.ChordalSquareO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestIntersection.HouseO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $line $outputA /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestIntersection.ChordalRoofO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}


	hadoop jar Subgraph.jar TestPartiton.ThreeTriangleO -D test.isOutput=true $outputA /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}

	hadoop jar Subgraph.jar TestIntersection.SolarSquareO -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $line /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}
	
	hadoop jar Subgraph.jar TestIntersection.SolarSquarePlusMO -D test.isOutput=true -D test.p=1 $line $outputA  /user/tri/temp1_${i}
	hadoop jar Subgraph.jar sTool.ReaderTest /user/tri/temp1_${i}
	hadoop fs -rm -r /user/tri/temp1_${i}


done
