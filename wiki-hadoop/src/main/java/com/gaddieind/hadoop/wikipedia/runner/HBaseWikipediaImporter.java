package com.gaddieind.hadoop.wikipedia.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gaddieind.hadoop.wikipedia.mapper.WikipediaMapper;

public class HBaseWikipediaImporter extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		/*if (args.length != 1) {
			System.err.println("Usage: HBaseWikipediaImporter <input>");
			return -1;
		}*/
		/*JobConf jc = new JobConf(getConf(), getClass());
		jc.setJobName("Wikipedia Input");
		jc.setMapperClass(WikipediaMapper.class);
		jc.setNumReduceTasks(0);
		jc.setInputFormat(WikipediaPageInputFormat.class);
		jc.setOutputFormat(NullOutputFormat.class);*/
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "node1.gaddieind.com:60000");
		conf.set("hbase.zookeeper.quorum", "node1.gaddieind.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		
		Job wikiJob = new Job(conf);
		wikiJob.setJobName("Wikipedia Input Job");
		TableMapReduceUtil.addDependencyJars(wikiJob);
		wikiJob.setJarByClass(HBaseWikipediaImporter.class);
		wikiJob.setMapperClass(WikipediaMapper.class);
		wikiJob.setOutputFormatClass(TableOutputFormat.class);
		wikiJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "wikipedia");
		wikiJob.setOutputKeyClass(ImmutableBytesWritable.class);
		wikiJob.setOutputValueClass(Writable.class);
		wikiJob.setNumReduceTasks(0);
		FileInputFormat.addInputPath(wikiJob, new Path("/input/enwiki-20121001-pages-articles.xml"));
		wikiJob.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new HBaseWikipediaImporter(), args);
		System.exit(exitCode);
	}

}
