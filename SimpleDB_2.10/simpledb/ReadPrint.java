import simpledb.tx.*;
import simpledb.server.SimpleDB;
import simpledb.file.Block;
import simpledb.buffer.*;

import static simpledb.file.Page.*;
import simpledb.file.*;

import java.util.*;
import java.io.*;


public class ReadPrint {

    public static void main(String[] args) {
        SimpleDB.init("demo0127");
        FileMgr fm = SimpleDB.fileMgr();
        BufferMgr bm = SimpleDB.bufferMgr();
        Transaction tx = new Transaction();
        for (int i = 0; i < 8; i++) {
            Block blk = new Block("demo.tbl", i);
            tx.pin(blk);
            int newVal = tx.getInt(blk, 4);
            tx.unpin(blk);
            System.out.println("Block number is " + blk.number());
            System.out.println("New value is " + newVal);
        }
        tx.commit();
    }
}