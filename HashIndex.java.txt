package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Collections;
import java.util.Iterator;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Edward Sciore
 */
public class HashIndex implements Index {
	public static int NUM_BUCKETS = 100;
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;

    public ArrayList<ArrayList<Integer>> indexFile;
    public int level = 0;
    public int nextB = 0;

	/**
	 * Opens a hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	public HashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
        // indexFile = new ArrayList<ArrayList<Integer>>(Collections.nCopies(NUM_BUCKETS, new ArrayList<Integer>()));
        indexFile = new ArrayList<ArrayList<Integer>>(NUM_BUCKETS);
        for (int i = 0; i < 100; i++) {
            indexFile.add(new ArrayList<Integer>());
        }
	}

    /**
	 * Private helper method to print current state.
	 */
     private void curState() {
         for (int i = 0; i < indexFile.size(); i++) {
             ArrayList<Integer> temp = indexFile.get(i);
             if (temp.size() == 0) continue;
             System.out.println("Bucket # is " + i);
             System.out.print("[ ");
             for (int k = 0; k < temp.size(); k++) {
                 if (k < 5) {
                     System.out.print(temp.get(k));
                     System.out.print(" ");
                 }
             }
             System.out.println("]");
             if (temp.size() > 5) {
                 System.out.println("Overflow for Bucket");
                 System.out.print("[ ");
                 for (int j = 5; j < temp.size(); j++) {
                     System.out.print(temp.get(j));
                     System.out.print(" ");
                 }
                 System.out.println("]");
             }
         }
         System.out.println("value for Level is: " + level);
         System.out.println("value for Next is: " + nextB);
     }

	/**
	 * Positions the index before the first index record
	 * having the specified search key.
	 * The method hashes the search key to determine the bucket,
	 * and then opens a table scan on the file
	 * corresponding to the bucket.
	 * The table scan for the previous bucket (if any) is closed.
	 * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		close();
        this.searchkey = searchkey;
        // get the real location
        int insertValue = (int)((IntConstant)searchkey).asJavaVal();
        int insertBucket = insertValue % (pow(level));
        if (insertBucket < nextB) {
            insertBucket = insertValue % (pow(level + 1));
        }
		String tblname = idxname + insertBucket;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
	}

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see simpledb.index.Index#next()
	 */
	public boolean next() {
		while (ts.next())
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}

	/**
	 * Retrieves the dataRID from the current record
	 * in the table scan for the bucket.
	 * @see simpledb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

    /**
	 * private helper method Power of 2
	 */
     private int pow(int val) {
         int result = 2;
         if (val == 0) return 1;
         for (int i = 1; i < val; i++) {
             result *= 2;
         }
         return result;
     }

     /**
 	 * private helper method expand
 	 */
     private void expand(int overBucket, Constant overVal, ArrayList<Integer> loc) {
         ArrayList<Integer> oldBucket = indexFile.get(nextB);
         ArrayList<Integer> expandBucket = new ArrayList<Integer>();
         ArrayList<Integer> overflowElements = new ArrayList<Integer>();
         for (int i = 5; i < loc.size(); i++) {
             overflowElements.add(loc.get(i));
         }
         int newLoc = nextB + pow(level);

        //  System.out.println("before");
        //  System.out.println(Arrays.toString(oldBucket.toArray()));
        //  System.out.println(Arrays.toString(expandBucket.toArray()));
        //  System.out.println(Arrays.toString(overflowElements.toArray()));

         ArrayList<Integer> total = new ArrayList<Integer>(oldBucket);
        //  if (overBucket == nextB) {
        //      total.addAll(overflowElements);
        //  }

        //  System.out.println(Arrays.toString(total.toArray()));

         oldBucket.clear();
         for (int i : total) {
             if ((i % pow(level + 1)) == nextB) {
                 oldBucket.add(i);
             } else {
                 expandBucket.add(i);
             }
         }

        //  System.out.println("after");
        //  System.out.println(Arrays.toString(oldBucket.toArray()));
        //  System.out.println(Arrays.toString(expandBucket.toArray()));
        //  System.out.println(Arrays.toString(overflowElements.toArray()));


         indexFile.set(newLoc, expandBucket);

         nextB = (nextB + 1) % pow(level);
         if (nextB == 0) level = level + 1;
        //  for (Iterator<Integer> iterator = loc.iterator(); iterator.hasNext();) {
        //     int cur = iterator.next();
        //     if (overflowElements.contains(cur)) {
        //         if (oldBucket.contains(cur) || expandBucket.contains(cur)) {
        //             iterator.remove();
        //         }
        //     }
        // }
     }

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);

        int insertValue = (int)((IntConstant)val).asJavaVal();
        int insertBucket = insertValue % (pow(level));
        if (insertBucket < nextB) {
            insertBucket = insertValue % (pow(level + 1));
        }
        //System.out.println(insertBucket);
        ArrayList<Integer> loc = indexFile.get(insertBucket);
        // System.out.println("before");
        // System.out.println(Arrays.toString(loc.toArray()));
        loc.add(insertValue);
        if (loc.size() > 5) {
            System.out.println("Before expand");
            curState();
            System.out.println("After expand");
            expand(insertBucket, val, loc);
            curState();
        }
        // System.out.println("after");
        // System.out.println(Arrays.toString(loc.toArray()));

		// curState();
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while(next())
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	public void close() {
		if (ts != null)
			ts.close();
	}

	/**
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb){
		return numblocks / HashIndex.NUM_BUCKETS;
	}
}
