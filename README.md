# Final Year Project

Requirements:\
    - Java 8+\
    - Python 3\
    - Apache Hadoop\
    - An already configured Hadoop cluster (can be standalone or distributed cluster)\

For a video demonstration of this program, please contact me through the given channels.

The datasets used in this program are available at:\
    https://www.kaggle.com/datasets/shibumohapatra/house-price \ 
    https://www.kaggle.com/datasets/corrieaar/disinformation-articles
    
This program can be run using the given JAR files which can be built. The .sh files provided can run the respective
 programs in a linux Z shell, however if these scripts do not work, these programs are built to work with the 
 hadoop jar command

The arguments for both program scripts are:
./program.sh  *input-directory* *output-directory*

In the case that these scripts do not work correctly, pass the following as the main classes in the hadoop jar
command:

For WordCount:
main.java.org.finalyearproject.wordcount.Driver

For DistributedGrep:
main.java.org.finalyearproject.distributedgrep.Driver

For FrequncyDistribution:
main.java.org.finalyearproject.frequencydistribution.GraphBuilder

bin/hadoop jar /home/user/git/PROJECT/Code/FrequencyDistribution/target/frequencydistribution-0.0.1-SNAPSHOT.jar main.java.org.finalyearproject.frequencydistribution.GraphBuilder /input /output

bin/hadoop jar /home/user/git/PROJECT/Code/kmeans/target/kmeans-1.0.0.jar main.java.org.finalyearproject.kmeans.Driver "HDFS Input" "HDFS Output" "Number of iterations"

For K-Means:
main.java.org.finalyearproject.kmeans.Driver

bin/hadoop jar /home/user/git/PROJECT/Code/kmeans/target/kmeans-1.0.0.jar main.java.org.finalyearproject.kmeans.Driver /input/housing /output "Number of iterations"

bin/hadoop jar /home/user/git/PROJECT/Code/kmeans/target/kmeans-1.0.0.jar main.java.org.finalyearproject.kmeans.Driver "HDFS Input" "HDFS Output" "Number of iterations"

This program has been tested on both a single node and multi node cluster, it is currently optimised for a four
datanode setup.
