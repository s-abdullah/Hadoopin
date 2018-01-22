Milestone 1
	Input file: data.txt
	Output file: part-r-0000.bin

	lid of commands to run file assuming inpput files is in HDFS directory /user/hduser/testerIn/

	1) export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
	2) hadoop com.sun.tools.javac.Main SocialRank.java
	3) jar cf wc.jar SocialRank*.class
	4) hadoop jar SocialRank.jar SociaRank <input format as stated in the assignment>
			*for complete run* input format: composite /user/hduser/testerIn/ /user/hduser/test/final /user/hduser/test/inter1 /user/hduser/test/inter2 /user/hduser/test/diffOut 1




