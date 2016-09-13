package org.hadoop.learn.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class TestHbaseJava {

	public static void main(String[] args) throws IOException {
		// Try to create a Table with 2 column family (position, position)
		HTableDescriptor descriptor = new HTableDescriptor("hotspots3");
		descriptor.addFamily(new HColumnDescriptor("position"));
		descriptor.addFamily(new HColumnDescriptor("inpositionfo"));
		Configuration config = HBaseConfiguration.create();
		try {
			// Create a HBaseAdmin
			config.set("hbase.zookeeper.quorum", "hadoopmaster");
			config.set("hbase.zookeeper.property.clientPort", "2181");
			HBaseAdmin admin = new HBaseAdmin(config);

			// Create table
			admin.createTable(descriptor);
			System.out.println("Table created");
		} catch (IOException e) {
			System.out.println("IOError: cannot create Table");
			e.printStackTrace();
		}
		Connection conn = ConnectionFactory.createConnection(config);
		Table yugsTable = conn.getTable(TableName.valueOf("hotspots3"));
		Scan scanner = new Scan();
		ResultScanner rScanner = yugsTable.getScanner(scanner);
		for (Result res : rScanner) {
			System.out.println(res);

		}

	}

}
