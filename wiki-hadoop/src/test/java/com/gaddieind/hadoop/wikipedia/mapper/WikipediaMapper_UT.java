package com.gaddieind.hadoop.wikipedia.mapper;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;


public class WikipediaMapper_UT {
	@Test
	public void processesValidRecord() throws Exception{
		File file = new File("C:\\enwiki-20121001-pages-articles.xml");
		InputStream in =FileUtils.openInputStream(file);
		InputStream is = new BufferedInputStream(in );
		byte[] bytes = new byte[90000];
		is.read(bytes);
		
		
		Text value = new Text(bytes);
		System.out.println(value.toString());
		
		
		DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
		// domFactory.setNamespaceAware(true); // never forget this!
		    DocumentBuilder builder = domFactory.newDocumentBuilder();
		    Document doc = builder.parse(new ByteArrayInputStream(value.toString().getBytes()));

		    XPathFactory factory = XPathFactory.newInstance();
		    XPath xpath = factory.newXPath();
		    XPathExpression expr = xpath.compile("//page/id/text()");
		    Object result = expr.evaluate(doc, XPathConstants.NODESET);
		    
		    NodeList nodes = (NodeList) result;
		    for (int i = 0; i < nodes.getLength(); i++) {
		        System.out.println(nodes.item(i).getNodeValue()); 
		    }
		
		// Temperature ^^^^^
		/*new MapDriver<LongWritable, Text, Text, Writable>()
				.withMapper(new WikipediaMapper()).withInputValue(value)
				.withOutput(new Text("1950"), new IntWritable(-11)).runTest();*/
	}
}
