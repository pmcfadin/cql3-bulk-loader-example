package com.datastax.bulkloader;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CompositeType.Builder;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

public class CQL3SimpleDataImportExample {

	static String filename;

	public static void main(String[] args) throws IOException {

		String keyspace = "test";
		File directory = new File(keyspace);
		if (!directory.exists())
			directory.mkdir();

		// Create a composite column with the types for each part
		List<AbstractType<?>> compositeColumnValues = new ArrayList<AbstractType<?>>();
		compositeColumnValues.add(IntegerType.instance);
		compositeColumnValues.add(UTF8Type.instance);
		CompositeType compositeColumn = CompositeType
				.getInstance(compositeColumnValues);

		SSTableSimpleUnsortedWriter testWriter = new SSTableSimpleUnsortedWriter(
				directory, new Murmur3Partitioner(), keyspace, "test", compositeColumn,
				null, 64);

		// Create a single timestamp for each insert
		long timestamp = System.currentTimeMillis() * 1000;

		Builder builder = compositeColumn.builder();

		testWriter.newRow(ByteBuffer.wrap("one".getBytes()));

		// First column has a blank column value
		builder.add(bytes(1));
		builder.add(bytes(""));
		testWriter.addColumn(builder.build(), bytes(""), timestamp);
		
		// Second column with a column value
		builder = compositeColumn.builder();
		builder.add(bytes(1));
		builder.add(bytes("c"));

		testWriter.addColumn(builder.build(), bytes("one"), timestamp);

		
		// Next row
		builder = compositeColumn.builder();

		testWriter.newRow(ByteBuffer.wrap("two".getBytes()));

		// First column has a blank column value
		builder.add(bytes(2));
		builder.add(bytes(""));
		testWriter.addColumn(builder.build(), bytes(""), timestamp);
		
		// Second column with a column value
		builder = compositeColumn.builder();
		builder.add(bytes(2));
		builder.add(bytes("c"));

		testWriter.addColumn(builder.build(), bytes("two"), timestamp);
		
		
		// Next row
		builder = compositeColumn.builder();

		testWriter.newRow(ByteBuffer.wrap("three".getBytes()));

		// First column has a blank column value
		builder.add(bytes(3));
		builder.add(bytes(""));
		testWriter.addColumn(builder.build(), bytes(""), timestamp);
		
		// Second column with a column value
		builder = compositeColumn.builder();
		builder.add(bytes(3));
		builder.add(bytes("c"));

		testWriter.addColumn(builder.build(), bytes("three"), timestamp);
		
		
		testWriter.close();
		System.exit(0);
	}

}
