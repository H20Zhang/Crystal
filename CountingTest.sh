prefix="/user/hzhang/subgraph"
input=(as) #need to specify your own input
declare -A edgeNumDic=(["as"]="11095298" ["wikiV"]="103689" ["eu-core"]="25571")

for i in ${input[@]}; do
	
	data=$i

	root="${prefix}/${data}"
	degreeEdgeTemp="${root}/countDegreeA_${data}"
	degreeEdge="${root}/countDegreeB_${data}"
	outputA="${root}/tri_${data}"
	outputB="${root}/four_${data}"
	bloom="${root}/bloom_${data}/bloom_file/bloom"
	plusA="${root}/preprocessPlus_${data}"
	plusB="${root}/preprocessFourPlus_${data}"
	line="${root}/line_${data}"

	jarFile=/home/tmp/Dropbox/SEPC/SubgraphTest/Subgraph.jar

	#run Square
	hadoop jar $jarFile sAlg.Square -libjars trove4j-3.0.3.jar  -D test.isEmitted=true -D test.p=1 $line $line

	#run chordal square pattern
	hadoop jar $jarFile sAlg.ChordalSquare -libjars trove4j-3.0.3.jar -D test.isEmitted=true $outputA

	#run house pattern
	hadoop jar $jarFile sAlg.HouseS -libjars trove4j-3.0.3.jar -D test.p=1 -D test.isEmitted=true $line $outputA

	#run chordal roof pattern 
	hadoop jar $jarFile sAlg.ChordalRoof -libjars trove4j-3.0.3.jar -D test.p=1 -D test.isEmitted=true $outputA $line

	#run three triangle
	hadoop jar $jarFile sAlg.ThreeTriangle -libjars trove4j-3.0.3.jar -D test.p=20 -D test.isEmitted=true $outputA 

	#run solar square
	hadoop jar $jarFile sAlg.SolarSquare -libjars trove4j-3.0.3.jar -D test.isEmitted=true -D test.p=1 $outputA $line

	#run near5clique
	hadoop jar $jarFile sAlg.Near5Clique -libjars trove4j-3.0.3.jar -Dtest.p=20 -D test.isEmitted=true $outputA

	#run quadTriangle
	hadoop jar $jarFile sAlg.QuadTriangle -libjars trove4j-3.0.3.jar -D test.isEmitted=true $outputA

	# run solar square plus
	hadoop jar $jarFile sAlg.SolarSquarePlus -libjars trove4j-3.0.3.jar -D test.isEmitted=true -D test.p=1 $outputA $line

done




	
