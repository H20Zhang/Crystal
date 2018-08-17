prefix="/user/hzhang/subgraph"
input=(wikiV)
declare -A edgeNumDic=(["as"]="11095298" ["wikiV"]="103689")

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
	tmp="${root}/tmp"

	#test writing square pattern
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.SquareO -libjars trove4j-3.0.3.jar -D test.isOutput=true -D test.round=1 -D test.memory=4000 $line $line $tmp

	#test writing chordal square
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.ChordalSquareO -libjars trove4j-3.0.3.jar -D test.isOutput=true -D test.p=1 -D test.memory=4000 $outputA $tmp

	#test writing house, might exist some problem
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.HouseSO -libjars trove4j-3.0.3.jar -D test.isOutput=true -D test.round=1 -D test.memory=4000 $outputA $line $tmp

	#test writing chordal roof
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.ChordalRoofO -libjars trove4j-3.0.3.jar -D test.isOutput=true -D test.round=1 -D test.memory=4000 $outputA $line $tmp

	#test writing three triangle
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.ThreeTriangleO -libjars trove4j-3.0.3.jar -D test.isOutput=true $outputA $tmp

	#test writing solar square
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.SolarSquareO -libjars trove4j-3.0.3.jar -D test.isOutput=true -D test.round=1 -D test.memory=4000 $outputA $line $tmp

	#test writing near5Clique
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.Near5CliqueO -libjars trove4j-3.0.3.jar -Dtest.isOutput=true -Dtest.p=20 $outputA $tmp

	#test writing quadTriangle
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.QuadTriangleO -libjars trove4j-3.0.3.jar -Dtest.isOutput=true $outputA $tmp

	#test writing solarSuqarePlus
	hadoop fs -rm -r $tmp
	hadoop jar Subgraph.jar sAlg.SolarSquarePlusO -libjars trove4j-3.0.3.jar -D test.isOutput=true -D test.round=1 $line $outputA  $tmp
	
	hadoop fs -rm -r $tmp
done