package com.hadoop.relativefrequency.stripes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import com.hadoop.relativefrequency.pairs.RelativeFrequencyMapper;

public class RelativeFrequencyTool extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new RelativeFrequencyTool(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Configuration conf = this.getConf();

		// Create job
		Job job = new Job(conf, "RelativeFrequencyComputationStripes");
		job.setJarByClass(RelativeFrequencyTool.class);

		// Setup MapReduce
		job.setMapperClass(RelativeFrequencyMapper.class);
		job.setReducerClass(RelativeFrequencyReducer.class);
		job.setPartitionerClass(RelativeFrequencyStripesPartitioner.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
