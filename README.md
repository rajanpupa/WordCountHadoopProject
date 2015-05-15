# WordCountHadoopProject
Word count map-reduce maven java project for Hadoop2.6.0 distributed computing.

This was initially just a word count code, but there are more things added to this project.

It is added with files to calculate the relative frequency of terms occuring in an array of items.
These are being solved by three different ways.
* Pairs approach (simplest mapper and reducer)
* Stripes approach 
* Hybrid approach

###There are three separate *.sh file above to run the different algorithms.

# Setup
You should have java 1.7 or above in your system, maven 3.3

* Clone the project
* package it 
```mvn package```


Then the jar file generated is ready for being used in hadoop.

call the hadoop specifying the Configuration file the input and output hdfs filesystem of hadoop.

see the *.sh files above for the code to run hadoop with the generated jar file.

Refer to : http://rajanpupa.blogspot.com/2015/05/hadoop-installation-and-first-map.html
