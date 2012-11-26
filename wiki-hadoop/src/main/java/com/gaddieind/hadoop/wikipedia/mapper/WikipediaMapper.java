package com.gaddieind.hadoop.wikipedia.mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class WikipediaMapper extends 
		Mapper<LongWritable, WikipediaPage, ImmutableBytesWritable, Writable> {

	private static final Logger LOG = Logger.getLogger(WikipediaMapper.class);

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
	};
	
	private static enum ColumnFamilies {
		ARTICLE_INFO, CONTENT
	};
	
	private static enum ColumnQualifiers {
		TITLE, CONTENT
	};

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM                     = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT                 = "hbase.zookeeper.property.clientPort";
	
	private static final Text articleName = new Text();
	private static final Text articleContent = new Text();
	private HTable table;

	@Override
    public void map(LongWritable offset, WikipediaPage page, Context context) 
    throws IOException {

		context.getCounter(PageTypes.TOTAL).increment(1);

		if (page.isRedirect()) {
			context.getCounter(PageTypes.REDIRECT).increment(1);

		} else if (page.isDisambiguation()) {
			context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
		} else if (page.isEmpty()) {
			context.getCounter(PageTypes.EMPTY).increment(1);
		} else if (page.isArticle()) {
			context.getCounter(PageTypes.ARTICLE).increment(1);

			if (page.isStub()) {
				context.getCounter(PageTypes.STUB).increment(1);
			}

			LOG.info("Found an article: " + page.getTitle());
			//Insert into HBase
			
			Put put = new Put(page.getDocid().getBytes());
			put.add(ColumnFamilies.ARTICLE_INFO.name().getBytes(), ColumnQualifiers.TITLE.name().getBytes(), page.getTitle().getBytes());
			put.add(ColumnFamilies.CONTENT.name().getBytes(), ColumnQualifiers.CONTENT.name().getBytes(), page.getContent().getBytes());
			
			
			try {
				context.write(new ImmutableBytesWritable(page.getDocid().getBytes()), put);
			} catch (InterruptedException e) {
				LOG.error("Mapper Screwed up", e);
			}
			
			
			//table.put(put);
			//articleName.set(page.getTitle().replaceAll("[\\r\\n]+", " "));
			//articleContent.set(page.getContent().replaceAll("[\\r\\n]+", " "));

			//output.collect(articleName, articleContent);
		} else {
			context.getCounter(PageTypes.NON_ARTICLE).increment(1);
		}
	}

	/*public void configure(JobConf jc) {
		super.configure(jc);
		// Create the HBase table client once up-front and keep it around
		// rather than create on each map invocation.
		try {
			Configuration hbConf = HBaseConfiguration.create();
			hbConf.set("hbase.master", "node1.gaddieind.com:60000");
			hbConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "node1.gaddieind.com");
			hbConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, "2181");
			this.table = new HTable(hbConf, "wikipedia");
		} catch (IOException e) {
			LOG.error("Something Failed", e);
			throw new RuntimeException("Failed HTable construction", e);
		}
	}*/

}
