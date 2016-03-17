import simpledb.tx.*;
import simpledb.server.SimpleDB;
import simpledb.file.Block;
import simpledb.buffer.*;
import simpledb.record.*;
import simpledb.index.*;
import simpledb.query.*;
import simpledb.index.hash.*;

import static simpledb.file.Page.*;
import simpledb.file.*;

import java.util.*;
import java.io.*;


public class testLinearHashing {
    private static Schema idxsch = new Schema();
    private static Transaction tx;

    public static void main(String[] args) {
        SimpleDB.init("studentdb");
        System.out.println("BUILD LINEAR HASH INDEX");
        tx = new Transaction();
        // Defines the schema for an index record for Col1 of the messy table
        idxsch.addIntField("dataval");
        idxsch.addIntField("block");
        idxsch.addIntField("id");
        // Builds a Linear Hash Index on Col1 of the messy table
        Index idx = new HashIndex("hashIdxTest", idxsch, tx);
        Plan p = new TablePlan("messy", tx);
        UpdateScan s = (UpdateScan) p.open();
        while (s.next()) {
            idx.insert(s.getVal("col1"), s.getRid());
        }
        s.close();
        idx.close();
        tx.commit();

        //test for git
        // Transaction tx2 = new Transaction();
        // Plan testPlan = new TablePlan("messy", tx2);
        // TableScan testScan = (TableScan) testPlan.open();
        // Index testIdx = new HashIndex("hashIdxTest", idxsch, tx2);
        // testIdx.beforeFirst(new IntConstant(102));
        // RID dataRid = testIdx.getDataRid();
        // testScan.moveToRid(dataRid);
        // System.out.println(testScan.getString("col2"));
        // testScan.close();
        // testIdx.close();
        // tx2.commit();
    }
}
