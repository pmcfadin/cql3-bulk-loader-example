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

/*
 * A very simple example of creating CQL3 compatible SSTables for use with bulk loader.
 * 
 * To use this example, you need to create a keyspace called 'test'
 * 
 * From cqlsh run the following:
 * 
 * CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
 * 
 * use test;
 * 
 * create table test (
 * 	a varchar,
 * 	b int,
 * 	c varchar,
 * 	primary key (a,b)
 * );
 * 
 * The code below will create data files that simulate the following CQL commands:
 * 
 * insert into test(a,b,c) values ('one',1,'one');
 * insert into test(a,b,c) values ('two',2,'two');
 * insert into test(a,b,c) values ('three',3,'three');
 * 
 * If you are learning how this works, it would be a good idea to look at how the data 
 * is stored using the cassandra-cli. Insert the above values using cqlsh, then do the 
 * following from cassandra-cli:
 * 
 * use test;
 * list test;
 * 
 * You will see this output:
 * 
 * Using default limit of 100
 * Using default column limit of 100
 * -------------------
 * RowKey: three
 * => (column=3:, value=, timestamp=1369671593542000)
 * => (column=3:c, value=7468726565, timestamp=1369671593542000)
 * -------------------
 * RowKey: one
 * => (column=1:, value=, timestamp=1369671593542000)
 * => (column=1:c, value=6f6e65, timestamp=1369671593542000)
 * -------------------
 * RowKey: two
 * => (column=2:, value=, timestamp=1369671593542000)
 * => (column=2:c, value=74776f, timestamp=1369671593542000)
 * 
 * 3 Rows Returned.
 * 
 * Use this output to see how it relates to the way we write data below.
 * 
*/
public class CQL3SimpleDataImportExample {

	public static void main(String[] args) throws IOException {

		String keyspace = "test";
		File directory = new File(keyspace);
		if (!directory.exists())
			directory.mkdir();

		/* 
		 * Create a composite column with the types for each part
		 * 
		 * Each column will have the types in order of the table creation command
		 * In this case we have an int then a varchar(UTF8 is the underlying type)
		 * Use the type mapping here: http://www.datastax.com/docs/1.2/cql_cli/cql_data_types
		 *
		*/
		
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

		// First row. RowKey = a ='one', b=1, c='one'
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

		
		// Next row. RowKey = a ='two', b=2, c='two'
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
		
		
		// Next row. RowKey = a ='three', b=3, c='three'
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
