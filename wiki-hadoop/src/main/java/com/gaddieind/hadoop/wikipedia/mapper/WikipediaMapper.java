package com.gaddieind.hadoop.wikipedia.mapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;


public class WikipediaMapper extends 
		Mapper<LongWritable, Text,  ImmutableBytesWritable, Writable> {

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
	
	private DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
	private XPathFactory factory = XPathFactory.newInstance();
	
	XPath xpath;
	XPathExpression idExpr;
    XPathExpression titleExpr;
    XPathExpression textExpr;
	
	public WikipediaMapper() throws XPathExpressionException{
		xpath = factory.newXPath();
		
		idExpr = xpath.compile("//page/id/text()");
	    titleExpr = xpath.compile("//page/title/text()");
	    textExpr = xpath.compile("//page/revision/text/text()");
	}

	@Override
    public void map(LongWritable offset, Text page, Context context) 
    throws IOException {

		domFactory.setNamespaceAware(true); 
		context.getCounter(PageTypes.TOTAL).increment(1);
		
		if(LOG.isDebugEnabled()){
			LOG.debug("The Page! " + page);
		}
		
		String docId = null;
		String title = null;
		String content = null;
		try{
		
		    DocumentBuilder builder = domFactory.newDocumentBuilder();
		    Document doc = builder.parse(new ByteArrayInputStream(StringUtils.trim(page.toString()).getBytes()));

		    
		    NodeList idNode = (NodeList) idExpr.evaluate(doc, XPathConstants.NODESET);
		    NodeList titleNode = (NodeList) titleExpr.evaluate(doc, XPathConstants.NODESET);
		    NodeList textNode = (NodeList) textExpr.evaluate(doc, XPathConstants.NODESET);
		    
		    docId = idNode.item(0).getNodeValue();
		    title = titleNode.item(0).getNodeValue();
		    content = textNode.item(0).getNodeValue();
		    
		    if(LOG.isDebugEnabled()){
		    	LOG.info("ID: " + docId);
		    	LOG.info("title: " + title);
		    	LOG.info("content: " + content);
		    }
		    
		}catch(Exception e){
			LOG.error("You Suck!", e);
		}

			//Insert into HBase
			Put put = new Put(docId.getBytes());
			put.add(ColumnFamilies.ARTICLE_INFO.name().getBytes(), ColumnQualifiers.TITLE.name().getBytes(), title.getBytes());
			put.add(ColumnFamilies.CONTENT.name().getBytes(), ColumnQualifiers.CONTENT.name().getBytes(), content.getBytes());
			
			try {
				context.write(new ImmutableBytesWritable(docId.getBytes()), put);
			} catch (InterruptedException e) {
				LOG.error("Mapper Screwed up", e);
			}
	}
}
