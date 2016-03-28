package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.index.Index;
import java.util.ArrayList;
import java.util.Arrays;
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
    public int count = 1;
    public static int bucketCount = 0;
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
        // indexfile is a debug purpose arraylist which has the same
        // state as the index file, it can help to print out the state
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
             bucketCount = i + 1;
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
         //System.out.println("Total bucket is: " + bucketCount);
     }

     /**
 	 * helper to print the final state
 	 */
     public void finalState() {
         for (int i = 0; i < bucketCount; i++) {
            String tblname = idxname + i;
     		TableInfo ti = new TableInfo(tblname, sch);
     		TableScan b = new TableScan(ti, tx);
             System.out.println(b.getName());
             b.beforeFirst();
             while (b.next()) {
                 System.out.println((b.getVal("dataval")).asJavaVal());
             }
             b.close();
             System.out.println("   ");
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
        // int insertValue = (int)((IntConstant)searchkey).asJavaVal();
        int insertValue;
        try {
            insertValue = (int)((IntConstant)searchkey).asJavaVal();
        } catch (ClassCastException e) {
            insertValue = searchkey.hashCode();
        }

        // System.out.println("!!!!!!!!");
        // System.out.println(insertValue);
        // System.out.println("level is "+level);

        int insertBucket = insertValue % (pow(level));

        //System.out.println("old insertBucket " + insertBucket);

        if (insertBucket < nextB) {
            insertBucket = insertValue % (pow(level + 1));
        }

        //System.out.println("new insertBucket " + insertBucket);

		String tblname = idxname + insertBucket;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
        // System.out.println("the table name is " + tblname);
        // System.out.println("?????????");
	}

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see simpledb.index.Index#next()
	 */
	public boolean next() {
		while (ts.next()) {
            if (ts.getVal("dataval").equals(searchkey)) {
                //System.out.println("find you!!!");
                return true;
            }
        }
        //System.out.println("cant find");
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
     private void expand(int overBucket, Constant overVal, RID rid) {
         ArrayList<Integer> oldBucket = indexFile.get(nextB);
         ArrayList<Integer> expandBucket = new ArrayList<Integer>();
         int newLoc = nextB + pow(level);
         if (newLoc + 1 > bucketCount) bucketCount = newLoc + 1;

         // new bucket
         String tblname = idxname + newLoc;
         TableInfo ti = new TableInfo(tblname, sch);
         TableScan newIndex = new TableScan(ti, tx);
         newIndex.beforeFirst();
         // old bucket
         tblname = idxname + nextB;
         TableInfo ti1 = new TableInfo(tblname, sch);
         TableScan oldIndex = new TableScan(ti1, tx);
         oldIndex.beforeFirst();
         // intermediate bucket
         tblname = idxname + "Backup" + count;
         count++;
         TableInfo ti2 = new TableInfo(tblname, sch);
         TableScan intIndex = new TableScan(ti2, tx);
         intIndex.beforeFirst();
         // copy old content in the new
         while (oldIndex.next()) {
            intIndex.insert();
     		intIndex.setInt("block", oldIndex.getInt("block"));
     		intIndex.setInt("id", oldIndex.getInt("id"));
     		intIndex.setVal("dataval", oldIndex.getVal("dataval"));
         }
         // clear old content
         oldIndex.beforeFirst();
         while (oldIndex.next()) {
             oldIndex.delete();
         }
         // begin redistribute
         oldIndex.beforeFirst();
         intIndex.beforeFirst();
         while (intIndex.next()) {
             int val = (int)((IntConstant)(intIndex.getVal("dataval"))).asJavaVal();
             if ((val % pow(level + 1)) == nextB) {
                oldIndex.insert();
          		oldIndex.setInt("block", intIndex.getInt("block"));
          		oldIndex.setInt("id", intIndex.getInt("id"));
          		oldIndex.setVal("dataval", intIndex.getVal("dataval"));
             } else {
                newIndex.insert();
           		newIndex.setInt("block", intIndex.getInt("block"));
           		newIndex.setInt("id", intIndex.getInt("id"));
           		newIndex.setVal("dataval", intIndex.getVal("dataval"));
             }
         }

        //  System.out.println("before");
        //  System.out.println(Arrays.toString(oldBucket.toArray()));
        //  System.out.println(Arrays.toString(expandBucket.toArray()));

         ArrayList<Integer> total = new ArrayList<Integer>(oldBucket);
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

         indexFile.set(newLoc, expandBucket);
         nextB = (nextB + 1) % pow(level);
         if (nextB == 0) level = level + 1;

         newIndex.close();
         oldIndex.close();
         intIndex.close();
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
        ts.close();

        int insertValue;
        try {
            insertValue = (int)((IntConstant)val).asJavaVal();
        } catch (ClassCastException e) {
            insertValue = val.hashCode();
        }

        int insertBucket = insertValue % (pow(level));
        if (insertBucket < nextB) {
            insertBucket = insertValue % (pow(level + 1));
        }
        ArrayList<Integer> loc = indexFile.get(insertBucket);

        // System.out.println("before");
        // System.out.println(Arrays.toString(loc.toArray()));

        loc.add(insertValue);
        if (loc.size() > 5) {
            System.out.println(" ");
            System.out.println("Before expand");
            curState();
            System.out.println("After expand");
            expand(insertBucket, val, rid);
            curState();
            System.out.println(" ");
        }

        // System.out.println("after");
        // System.out.println(Arrays.toString(loc.toArray()));

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
		return numblocks / bucketCount;
	}

}
