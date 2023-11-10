#!/bin/bash
cd $1
if [ $? -ne 0 ] || [[ ! $1 ]]; then
    echo "Invalid directory '${1}' provided! Could not cd."
    echo "Usage: bash batch-test.sh src/folder"
    exit 1
fi

paxosTests=("TestBasic" "TestDeaf" "TestForget" "TestManyForget" "TestForgetMem" "TestRPCCount" "TestMany" "TestOld" "TestManyUnreliable" "TestPartition" "TestLots")
kvpaxosTests=("TestBasic" "TestDone" "TestPartition" "TestUnreliable" "TestHole" "TestManyPartition")
shardmasterTests=("TestBasic" "TestUnreliable" "TestFreshQuery")

# How many times to run each test case (eg 50)
num_executions=50

echo "Enter $1"
echo "Running these tests ${num_executions} times: ${@:2}"

runTests() {
  echo $1":"
  count=0
  runs=0
  for i in $(seq 1 $num_executions)
  do
    echo -ne "Passed: $count/$runs times"'\r';
    go test -run "^${1}$" -timeout 2m > ./log-${1}-${i}.txt
    result=$(grep -E '^PASS$' log-${1}-${i}.txt| wc -l)
    count=$((count + result))
    runs=$((runs + 1))
    if [ $result -eq 1 ]
    then
      rm ./log-${1}-${i}.txt
    else
      ../../extract ./log-${1}-${i}  # Updated line
      rm ./log-${1}-${i}.txt
    fi
  done
  echo -ne "Passed: $count/$runs times"'\n';
}

if [ $1 = src/paxos ] || [ $1 = src/paxos/ ]
then
  for t in ${paxosTests[@]}
  do
    runTests $t
  done
elif [ $1 = src/kvpaxos ] || [ $1 = src/kvpaxos/ ]
then
  for t in ${kvpaxosTests[@]}
  do
    runTests $t
  done
elif [ $1 = src/shardmaster  ] || [ $1 = src/shardmaster/ ]
then
  for t in ${shardmasterTests[@]}
  do
    runTests $t
  done
else
  echo "Usage: bash batch-test.sh src/folder"
fi
