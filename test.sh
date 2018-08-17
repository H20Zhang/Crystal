#!/usr/bin/env bash 


SECONDS=0
# do some work

echo execute preprocessing
./Preprocess.sh

duration=$SECONDS
echo "preprocessing $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."


SECONDS=0
echo execute counting
./CountingTest.sh
duration=$SECONDS
echo "counting $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."
# ./WritingTest.sh



