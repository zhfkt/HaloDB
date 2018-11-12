package com.oath.halodb;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import com.google.common.primitives.Ints;

public class TestMain {

	public static void main(String[] args) throws HaloDBException, IOException {
		// TODO Auto-generated method stub

		// Open a db with default options.
		HaloDBOptions options = new HaloDBOptions();

		// size of each data file will be 1GB.
		options.setMaxFileSize(1024 * 1024 * 1024);

		// the threshold at which page cache is synced to disk.
		// data will be durable only if it is flushed to disk, therefore
		// more data will be lost if this value is set too high. Setting
		// this value too low might interfere with read and write performance.
		options.setFlushDataSizeBytes(10 * 1024 * 1024);

		// The percentage of stale data in a data file at which the file will be
		// compacted.
		// This value helps control write and space amplification. Increasing this value
		// will
		// reduce write amplification but will increase space amplification.
		// This along with the compactionJobRate below is the most important setting
		// for tuning HaloDB performance. If this is set to x then write amplification
		// will be approximately 1/x.
		options.setCompactionThresholdPerFile(0.7);

		// Controls how fast the compaction job should run.
		// This is the amount of data which will be copied by the compaction thread per
		// second.
		// Optimal value depends on the compactionThresholdPerFile option.
		options.setCompactionJobRate(50 * 1024 * 1024);

		// Setting this value is important as it helps to preallocate enough
		// memory for the off-heap cache. If the value is too low the db might
		// need to rehash the cache. For a db of size n set this value to 2*n.
		options.setNumberOfRecords(100_000_000);

		// Delete operation for a key will write a tombstone record to a tombstone file.
		// the tombstone record can be removed only when all previous version of that
		// key
		// has been deleted by the compaction job.
		// enabling this option will delete during startup all tombstone records whose
		// previous
		// versions were removed from the data file.
		options.setCleanUpTombstonesDuringOpen(true);

		// HaloDB does native memory allocation for the in-memory index.
		// Enabling this option will release all allocated memory back to the kernel
		// when the db is closed.
		// This option is not necessary if the JVM is shutdown when the db is closed, as
		// in that case
		// allocated memory is released automatically by the kernel.
		// If using in-memory index without memory pool this option,
		// depending on the number of records in the database,
		// could be a slow as we need to call _free_ for each record.
		options.setCleanUpInMemoryIndexOnClose(false);

		// ** settings for memory pool **
		options.setUseMemoryPool(true);

		// Hash table implementation in HaloDB is similar to that of ConcurrentHashMap
		// in Java 7.
		// Hash table is divided into segments and each segment manages its own native
		// memory.
		// The number of segments is twice the number of cores in the machine.
		// A segment's memory is further divided into chunks whose size can be
		// configured here.
		options.setMemoryPoolChunkSize(2 * 1024 * 1024);

		// using a memory pool requires us to declare the size of keys in advance.
		// Any write request with key length greater than the declared value will fail,
		// but it
		// is still possible to store keys smaller than this declared size.
		options.setFixedKeySize(8);

		// Represents a database instance and provides all methods for operating on the
		// database.
		HaloDB db = null;

		// The directory will be created if it doesn't exist and all database files will
		// be stored in this directory
		String directory = "dbfiles";

		// Open the database. Directory will be created if it doesn't exist.
		// If we are opening an existing database HaloDB needs to scan all the
		// index files to create the in-memory index, which, depending on the db size,
		// might take a few minutes.
		db = HaloDB.open(directory, options);

		// key and values are byte arrays. Key size is restricted to 128 bytes.
		byte[] key1 = Ints.toByteArray(200);
		byte[] value1 = "Value for key 1".getBytes();

		byte[] key2 = Ints.toByteArray(300);
		byte[] value2 = "Value for key 2".getBytes();

		// add the key-value pair to the database.

		for (int i = 0; i < 1000; i++) {
			
			db.put(key1, value1);	

		}

		

		/*
		 * 
		 * db.put(key1, value1); db.put(key2, value2);
		 * 
		 * // read the value from the database. value1 = db.get(key1); value2 =
		 * db.get(key2);
		 * 
		 * // delete a key from the database. db.delete(key1);
		 */

		// Open an iterator and iterate through all the key-value records.
		HaloDBIterator iterator = db.newIterator();
		while (iterator.hasNext()) {
			Record record = iterator.next();
			System.out.println(Ints.fromByteArray(record.getKey()));
			System.out.println(new String(record.getValue()));
		}

		// get stats and print it.
		HaloDBStats stats = db.stats();
		System.out.println(stats.toString());

		// reset stats
		db.resetStats();

		// Close the database.
		db.close();

	}

}
