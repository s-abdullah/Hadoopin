# Hadoopin
MapReduce implemented with hadoop in an attempt to see how Social ranking algorithms work. SocialRank is very similar to PageRank, except that it operates on social network graphs instead of hyperlink graphs.


Input format: A social graph is initially available as a list of edges. A file that contains one line for
each link, and each line contains a pair of numbers that represent the vertices that
are connected by the link.
Output format: A file that contains the social rank of each individual in the social network. There should be one line for each vertex, and each line should contain the vertex identifier and the social rank.

SocialRank cannot be implemented efficiently with a single MapReduce job. However, each round can be implemented as a separate job in an iterative process. Thus, the output of round k will be used asthe input of round k+1. 
In addition, three additional types of jobs are implemented: One for converting the input data into our intermediate format, one for computing how much the rank values have changed from one round to the next, and one for converting the intermediate format into the output format.
