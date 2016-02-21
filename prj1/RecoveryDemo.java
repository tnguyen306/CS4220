import simpledb.tx.*;
import simpledb.server.SimpleDB;
import simpledb.file.Block;
import simpledb.buffer.*;

import static simpledb.file.Page.*;
import simpledb.file.*;

import java.util.*;
import java.io.*;


public class RecoveryDemo {

 public static void main(String[] args) throws IOException {
      
      SimpleDB.init("demo0127");

      FileMgr fm = SimpleDB.fileMgr();

      BufferMgr bm = SimpleDB.bufferMgr();

      System.out.println("Initialize Database State");

      Transaction tx = new Transaction();

      
 // INITIALIZE Blocks 0 through 7 in the file demo.tbl 
 // Print for each block: Transaction number, block number and new value 

      for (int i = 0; i < 8; i++) {
        int newVal = 100 * (i + 1);
        Block blk = new Block("demo.tbl", i);
        tx.pin(blk);
        tx.setInt(blk, 4, newVal);
        tx.unpin(blk);
        System.out.println("Transaction number is " + tx.getNum());
        System.out.println("Block number is " + blk.number());
        System.out.println("New value is " + newVal);
      }

      tx.commit();

     

      TestA t1 = new TestA();
      Thread th1 = new Thread(t1);
      th1.start();

      TestB t2 = new TestB();
      Thread th2 = new Thread(t2);
      th2.start();

      TestC t3 = new TestC();
      Thread th3 = new Thread(t3);
      th3.start();

      try {
         th1.join();
         th2.join();
         th3.join();
      }
      catch (InterruptedException e) {};

      tx = new Transaction();
      tx.recover();
      System.out.println("End  Recovery Test" );
     }
 }

class TestA implements Runnable {
   public void run() {
           
      Transaction tx1 = new Transaction();
        
  // UPDATE the value at location 4 in block 0 to 1000      
  // Print Transaction number, block number and new value 
      

  // UPDATE the value at location 4 in block 1 to 2000       
  // Print Transaction number, block number and new value   

  // UPDATE the value at location 4 in block 2 to 3000       
  // Print Transaction number, block number and new value   
 
   // UPDATE the value at location 4 in block 3 to 4000      
   // Print Transaction number, block number and new value  

      for (int i = 0; i < 4; i++) {
        int newVal = 1000 * (i + 1);
        Block blk = new Block("demo.tbl", i);
        tx1.pin(blk);
        tx1.setInt(blk, 4, newVal);
        tx1.unpin(blk);
        System.out.println("Transaction number is " + tx1.getNum());
        System.out.println("Block number is " + blk.number());
        System.out.println("New value is " + newVal);
      }

      tx1.commit();
     }
 }


class TestB implements Runnable {
   public void run() {
     

      Transaction tx = new Transaction();
     
      // UPDATE the value at location 4 in block 4 to 5000      
      // Print Transaction number, block number and new value  

      
      // UPDATE the value at location 4 in block 5 to 6000      
      // Print Transaction number, block number and new value  

      for (int i = 4; i < 6; i++) {
        int newVal = 1000 * (i + 1);
        Block blk = new Block("demo.tbl", i);
        tx.pin(blk);
        tx.setInt(blk, 4, newVal);
        tx.unpin(blk);
        System.out.println("Transaction number is " + tx.getNum());
        System.out.println("Block number is " + blk.number());
        System.out.println("New value is " + newVal);
      }

      tx.rollback(); 
  }
 }


class TestC implements Runnable {
   public void run() {
     
      Transaction tx = new Transaction();

      // UPDATE the value at location 4 in block 6 to 7000      
      // Print Transaction number, block number and new value  

     
      // UPDATE the value at location 4 in block 7 to 8000      
      // Print Transaction number, block number and new value  

      for (int i = 6; i < 8; i++) {
        int newVal = 1000 * (i + 1);
        Block blk = new Block("demo.tbl", i);
        tx.pin(blk);
        tx.setInt(blk, 4, newVal);
        tx.unpin(blk);
        System.out.println("Transaction number is " + tx.getNum());
        System.out.println("Block number is " + blk.number());
        System.out.println("New value is " + newVal);
      }

      tx.rollback(); 
  }
 }

