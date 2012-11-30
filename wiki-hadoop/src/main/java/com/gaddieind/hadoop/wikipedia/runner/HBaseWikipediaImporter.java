package com.gaddieind.hadoop.wikipedia.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.gaddieind.hadoop.inputformat.XmlInputFormat;
import com.gaddieind.hadoop.wikipedia.mapper.WikipediaMapper;

public class HBaseWikipediaImporter{

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "node1.gaddieind.com:60000");
		conf.set("hbase.zookeeper.quorum", "node1.gaddieind.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");


		JobConf jc = new JobConf(conf, HBaseWikipediaImporter.class);
		jc.setJobName("Wikipedia Input");
		jc.set("xmlinput.start", "<page>");
		jc.set("xmlinput.end", "</page>");
		
		Job wikiJob = new Job(jc);
		wikiJob.setJobName("Wikipedia Input Job");
		TableMapReduceUtil.addDependencyJars(wikiJob);
		wikiJob.setJarByClass(HBaseWikipediaImporter.class);
		wikiJob.setInputFormatClass(XmlInputFormat.class);
		wikiJob.setMapperClass(WikipediaMapper.class);
		
		wikiJob.setOutputFormatClass(TableOutputFormat.class);
		wikiJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "wikipedia");
		wikiJob.setOutputKeyClass(ImmutableBytesWritable.class);
		wikiJob.setOutputValueClass(Writable.class);
		wikiJob.setNumReduceTasks(0);
		FileInputFormat.addInputPath(wikiJob, new Path("/input/enwiki-20121001-pages-articles.xml"));
		
		 System.exit(wikiJob.waitForCompletion(true) ? 0 : 1);
	}
}
