- Part0 (Preprocessing):
  - Please run the function in iPython notebook: preprocessing.ipynb, which will 
    clean all the data for following questions.

********************************************************************************

Part1 (Q1):

- a: 
  - Command to run: /Your/Hadoop/Path/bin/hadoop jar part1/vc.jar VoterCount input a 
  - Command short: bin/hadoop jar part1/vc.jar VoterCount input a 
  - input: input file folder 
  - a: output file folder (Don't create it, hadoop will create it automatically)

- b: 
  - Command to run: /Your/Hadoop/Path/bin/hadoop jar part1/vm.jar VoterMean input b
  - Command short: bin/hadoop jar part1/vm.jar VoterMean input b
  - input: input file folder 
  - b: output file folder (Don't create it, hadoop will create it automatically)

********************************************************************************

Part2 (Q2):

- a: 
  - Command to run: /Your/Hadoop/Path/bin/hadoop jar part2/wc.jar WordCount input a 
  - Command short: bin/hadoop jar part2/wc.jar WordCount input a 
  - input: input file folder 
  - a: output file folder (Don't create it, hadoop will create it automatically)

- b: 
  - Command to run: /Your/Hadoop/Path/bin/hadoop jar part2/pc.jar PairCount input b
  - Command short: bin/hadoop jar part2/pc.jar PairCount input b
  - input: input file folder 
  - b: output file folder (Don't create it, hadoop will create it automatically)

********************************************************************************

Part3 (Q3):

- a: 
  - Command to run: /Your/Hadoop/Path/bin/hadoop jar part3/rt.jar ReturnTax input output
  - Command short: bin/hadoop jar part3/rt.jar ReturnTax input output
  - input: input file folder 
  - output: output file folder (Don't create it, hadoop will create it automatically)