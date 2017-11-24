package com.chimpler.hive.solr;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;

public class SolrWriter implements RecordWriter {
	private SolrTable table;
	
	public SolrWriter(String url, int numOutputBufferRows) {
		this.table = new SolrTable(url);
        if (numOutputBufferRows > 0) {
        	table.setNumInputBufferRows(numOutputBufferRows);
        }

	}

	@Override
	public void close(boolean abort) throws IOException {
		if (!abort) {
			table.commit();
		} else {
			table.rollback();
		}
	}

	@Override
	public void write(Writable w) throws IOException {
		MapWritable map = (MapWritable) w;
		SolrInputDocument doc = new SolrInputDocument();
		for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
			String key = entry.getKey().toString();
			String text = getStringValue(entry.getValue());
			// skip null values
			if (text == null) {
				continue;
			}
			if (text.contains("null")) {
				continue;
			}
			// is this string is in a format we support '[0|1|2|3]'
			if (text.contains("|")) {
		           String[] items = text.split("\\|");
	                   for (String val : items) {
                              if (StringUtils.isNotBlank(val)) {
                                doc.addField(key, val);
                              }
                           }
			} else {
				// single value field
				doc.setField(key, text);
			}
		}
		table.save(doc);
	}

	private String getStringValue(Writable w) {
		if (w == null) {
			return null;
		}
		// treat as string
		return w.toString();
	}
}
