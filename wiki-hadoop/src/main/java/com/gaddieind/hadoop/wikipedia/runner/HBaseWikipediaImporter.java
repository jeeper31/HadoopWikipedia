package com.gaddieind.hadoop.wikipedia.runner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gaddieind.hadoop.wikipedia.mapper.WikipediaMapper;

import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;

public class HBaseWikipediaImporter extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		/*if (args.length != 1) {
			System.err.println("Usage: HBaseWikipediaImporter <input>");
			return -1;
		}*/
		JobConf jc = new JobConf(getConf(), getClass());
		jc.setJobName("Wikipedia Input");
		FileInputFormat.addInputPath(jc, new Path("/input/enwiki-20121001-pages-articles.xml"));
		jc.setMapperClass(WikipediaMapper.class);
		jc.setNumReduceTasks(0);
		jc.setInputFormat(WikipediaPageInputFormat.class);
		jc.setOutputFormat(NullOutputFormat.class);
		
		Job wikiJob = new Job(jc);
		wikiJob.setJobName("Wikipedia Input Job");
		TableMapReduceUtil.addDependencyJars(wikiJob);
		wikiJob.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new HBaseWikipediaImporter(), args);
		System.exit(exitCode);
	}

}
