# Social Rank using Hadoop
MapReduce implemented with hadoop in an attempt to see how Social ranking algorithms work. SocialRank is very similar to PageRank, except that it operates on social network graphs instead of hyperlink graphs.


Input format: A social graph is initially available as a list of edges. A file that contains one line for
each link, and each line contains a pair of numbers that represent the vertices that
are connected by the link.
Output format: A file that contains the social rank of each individual in the social network. There should be one line for each vertex, and each line should contain the vertex identifier and the social rank.

SocialRank cannot be implemented efficiently with a single MapReduce job. However, each round can be implemented as a separate job in an iterative process. Thus, the output of round k will be used asthe input of round k+1. 
In addition, three additional types of jobs are implemented: One for converting the input data into our intermediate format, one for computing how much the rank values have changed from one round to the next, and one for converting the intermediate format into the output format.

## Instruction to Run
	Input file: data.txt
	Output file: part-r-0000.bin

	list of commands to run file assuming inpput files is in HDFS directory /user/hduser/testerIn/

	1) export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
	2) hadoop com.sun.tools.javac.Main SocialRank.java
	3) jar cf wc.jar SocialRank*.class
	4) hadoop jar SocialRank.jar SociaRank <input format as stated in the assignment>
			*for complete run* input format: composite /user/hduser/testerIn/ /user/hduser/test/final /user/hduser/test/inter1 /user/hduser/test/inter2 /user/hduser/test/diffOut 1

