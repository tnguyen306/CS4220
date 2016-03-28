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
        //((HashIndex) idx).finalState();
        s.close();

        TableScan testScan = (TableScan) p.open();
        for (int i = 0; i < args.length; i++) {
            try {
                int toFind = Integer.parseInt(args[i]);
                idx.beforeFirst(new IntConstant(toFind));
                boolean flag = idx.next();
                if (flag == true) {
                    RID dataRid = idx.getDataRid();
                    testScan.moveToRid(dataRid);
                    System.out.println(testScan.getString("col2"));
                } else {
                    System.out.println("No associated data, print the key: " + toFind);
                }
            } catch (NumberFormatException e) {
                System.err.println("Argument" + args[i] + " must be an integer.");
                System.exit(1);
            }
        }
        testScan.close();

        idx.close();
        tx.rollback();
    }
}
