package com.chimpler.hive.solr;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
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
			Object val = getObjectFromWritable(entry.getValue());
			// skip null values
			if (val == null) {
				continue;
			}
			// check if this is a multi-value field
			if (val instanceof String) {
				String token = ((String)val).trim();
				// is this string in hives array format '["0","1","2","3"]'
				if (token.contains(",") && token.startsWith("[") && token.endsWith("]")) {
					String[] items = token.replace("[", "").replace("]", "").replace("\"", "").replace("'", "").split(",");
					for (String item : items) {
						doc.addField(key, item.trim());
					}
				} else {
					// single value field
					doc.setField(key, val);
				}
			} else {
				doc.setField(key, val);
			}
		}
		table.save(doc);
	}

	private Object getObjectFromWritable(Writable w) {
		if (w == null) {
			return null;
		}
		try {
			if (w instanceof IntWritable) {
				// int
				return ((IntWritable) w).get();
			} else if (w instanceof ShortWritable) {
				// short
				return ((ShortWritable) w).get();
			} else if (w instanceof ByteWritable) {
				// byte
				return ((ByteWritable) w).get();
			} else if (w instanceof BooleanWritable) {
				// boolean
				return ((BooleanWritable) w).get();
			} else if (w instanceof LongWritable) {
				// long
				return ((LongWritable) w).get();
			} else if (w instanceof FloatWritable) {
				// float
				return ((FloatWritable) w).get();
			} else if (w instanceof DoubleWritable) {
				// double
				return ((DoubleWritable) w).get();
			}else if (w instanceof NullWritable) {
				//null
				return null;
			} else {
				// treat as string
				return w.toString();
			}
		} catch (Exception ex) {
			return w.toString();
		}
	}
}