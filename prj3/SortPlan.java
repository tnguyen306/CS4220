package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Plan p;
   private Transaction tx;
   private Schema sch;
   private RecordComparator comp;
   //counter for merge Iteration
   private int count = 0;
   private int k;

   /**
    * Creates a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    */
   public SortPlan(Plan p, List<String> sortfields, Transaction tx, int kruns) {
      this.p = p;
      this.tx = tx;
      sch = p.schema();
      k = kruns;
      comp = new RecordComparator(sortfields);
   }

   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      src.close();
      while (runs.size() > k) {
          count++;
          System.out.println("*********************************");
          System.out.println("Merge Iteration " + count);
          System.out.println("Number of Runs to be Merged is " + runs.size());
          System.out.println(" ");
          runs = doAMergeIteration(runs);
      }
    //   System.out.println("DEBUG:");
    //   System.out.println(runs.size());
    //   System.out.println(" ");
      return new SortScan(runs, comp);
   }

   /**
    * Returns the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // does not include the one-time cost of sorting
      Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
      return mp.blocksAccessed();
   }

   /**
    * Returns the number of records in the sorted table,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }

   /**
    * Returns the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }

   /**
    * Returns the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }

   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<TempTable>();
      src.beforeFirst();
      if (!src.next()) {
          return temps;
      }
      System.out.println("Starting A New Run");
      TempTable currenttemp = new TempTable(sch, tx);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan)) {
          if (comp.compare(src, currentscan) < 0) {
              // print out the old contents
              currentscan.beforeFirst();
              while (currentscan.next()) {
                  System.out.println(currentscan.getVal("col2") +
                    " "+ currentscan.getVal("col1"));
              }
              System.out.println(" ");
              // start a new run
              System.out.println("Starting A New Run");
              currentscan.close();
              currenttemp = new TempTable(sch, tx);
              temps.add(currenttemp);
              currentscan = (UpdateScan) currenttemp.open();
          }
      }
      // print the very last run's contents
      currentscan.beforeFirst();
      while (currentscan.next()) {
          System.out.println(currentscan.getVal("col2") +
            " " + currentscan.getVal("col1"));
      }
      System.out.println(" ");
      currentscan.close();
      System.out.println("Number of Runs is " + temps.size());
      System.out.println(" ");
      return temps;
   }

   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<TempTable>();
      // if there are at least k elements in runs, merge continues
      // else merge ends
      while (runs.size() > (k - 1)) { // at least k-1 runs
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         TempTable addMe;

         if (k == 2) {
             addMe = mergeTwoRuns(p1, p2);
         } else if (k == 3) {
             TempTable p3 = runs.remove(0);
            // System.out.println("DEBUG:");
            // System.out.println("Three temp table cases");
            // System.out.println(" ");
            addMe = mergeThreeRuns(p1, p2, p3);
         } else {
             TempTable p4 = runs.remove(0);
             TempTable p5 = runs.remove(0);
             addMe = mergeFourRuns(p1, p2, p4, p5);
         }

         result.add(addMe);
         // print the merged runs
         System.out.println("Merged Run");
         UpdateScan currentscan = addMe.open();
         currentscan.beforeFirst();
         while (currentscan.next()) {
             System.out.println(currentscan.getVal("col2") +
                " " + currentscan.getVal("col1"));
         }
         System.out.println(" ");
         currentscan.close();
      }
      // add the last fews
      if (runs.size() > 0) {
          TempTable theFirst;

          if (runs.size() == 1) {
              theFirst = runs.get(0);
          } else if (runs.size() == 2) {
            //   System.out.println("DEGUB: ");
            //   System.out.println(runs.size());
            //   System.out.println(" ");
              TempTable tp1 = runs.remove(0);
              TempTable tp2 = runs.remove(0);
              theFirst = mergeTwoRuns(tp1, tp2);
          } else {
              TempTable ttp1 = runs.remove(0);
              TempTable ttp2 = runs.remove(0);
              TempTable ttp3 = runs.remove(0);
              theFirst = mergeThreeRuns(ttp1, ttp2, ttp3);
          }
          result.add(theFirst);
          System.out.println("Merged Run");
          UpdateScan lastScan = theFirst.open();
          lastScan.beforeFirst();
          while (lastScan.next()) {
              System.out.println(lastScan.getVal("col2") +
              " " + lastScan.getVal("col1"));
          }
          System.out.println(" ");
          lastScan.close();
      }
      return result;
   }

   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      TempTable result = new TempTable(sch, tx);
      UpdateScan dest = result.open();

      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      while (hasmore1 && hasmore2)
         if (comp.compare(src1, src2) < 0)
         hasmore1 = copy(src1, dest);
      else
         hasmore2 = copy(src2, dest);

      if (hasmore1)
         while (hasmore1)
         hasmore1 = copy(src1, dest);
      else
         while (hasmore2)
         hasmore2 = copy(src2, dest);
      src1.close();
      src2.close();
      dest.close();
      return result;
   }

   /*helper method to merge three tables*/
   private TempTable mergeThreeRuns(TempTable p1, TempTable p2, TempTable p3) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      Scan src3 = p3.open();
      TempTable result = new TempTable(sch, tx);
      UpdateScan dest = result.open();

      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      boolean hasmore3 = src3.next();

        while (hasmore1 && hasmore2 && hasmore3) {
           if (comp.compare(src1, src2) < 0) {
               if (comp.compare(src1, src3) < 0) {
                   hasmore1 = copy(src1, dest);
               } else {
                   hasmore3 = copy(src3, dest);
               }
           }
           else {
               if (comp.compare(src2, src3) < 0) {
                   hasmore2 = copy(src2, dest);
               } else {
                   hasmore3 = copy(src3, dest);
               }
           }
        }

        if (hasmore1 && hasmore2) {
            while (hasmore1 && hasmore2) {
                if (comp.compare(src1, src2) < 0) {
                    hasmore1 = copy(src1, dest);
                }
                else {
                    hasmore2 = copy(src2, dest);
                }
            }
        } else if (hasmore1 && hasmore3) {
            while (hasmore1 && hasmore3) {
                if (comp.compare(src1, src3) < 0) {
                    hasmore1 = copy(src1, dest);
                }
                else {
                    hasmore3 = copy(src3, dest);
                }
            }
        } else if (hasmore2 && hasmore3) {
            while (hasmore2 && hasmore3) {
                if (comp.compare(src2, src3) < 0) {
                    hasmore2 = copy(src2, dest);
                }
                else {
                    hasmore3 = copy(src3, dest);
                }
            }
        }

        if (hasmore1) {
            while (hasmore1) {
                hasmore1 = copy(src1, dest);
            }
        }
        else if (hasmore2) {
            while (hasmore2) {
                hasmore2 = copy(src2, dest);
            }
        } else {
            while (hasmore3) {
                hasmore3 = copy(src3, dest);
            }
        }

      src1.close();
      src2.close();
      src3.close();
      dest.close();
      return result;
   }

   /*helper method to merge four tables*/
   private TempTable mergeFourRuns(TempTable p1, TempTable p2, TempTable p3, TempTable p4) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      Scan src3 = p3.open();
      Scan src4 = p4.open();
      TempTable result = new TempTable(sch, tx);
      UpdateScan dest = result.open();

      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      boolean hasmore3 = src3.next();
      boolean hasmore4 = src4.next();

      while (hasmore1 && hasmore2 && hasmore3 && hasmore4) {
          if (comp.compare(src1, src2) < 0) {
              if (comp.compare(src3, src4) < 0) {
                  if (comp.compare(src1, src3) < 0) {
                      hasmore1 = copy(src1, dest);
                  } else {
                      hasmore3 = copy(src3, dest);
                  }
              } else {
                  if (comp.compare(src1, src4) < 0) {
                      hasmore1 = copy(src1, dest);
                  } else {
                      hasmore4 = copy(src4, dest);
                  }
              }
          } else {
              if (comp.compare(src3, src4) < 0) {
                  if (comp.compare(src2, src3) < 0) {
                      hasmore2 = copy(src2, dest);
                  } else {
                      hasmore3 = copy(src3, dest);
                  }
              } else {
                  if (comp.compare(src2, src4) < 0) {
                      hasmore2 = copy(src2, dest);
                  } else {
                      hasmore4 = copy(src4, dest);
                  }
              }
          }
      }

      if (hasmore1 && hasmore2 && hasmore3) {
          while (hasmore1 && hasmore2 && hasmore3) {
             if (comp.compare(src1, src2) < 0) {
                 if (comp.compare(src1, src3) < 0) {
                     hasmore1 = copy(src1, dest);
                 } else {
                     hasmore3 = copy(src3, dest);
                 }
             }
             else {
                 if (comp.compare(src2, src3) < 0) {
                     hasmore2 = copy(src2, dest);
                 } else {
                     hasmore3 = copy(src3, dest);
                 }
             }
          }
      } else if (hasmore1 && hasmore2 && hasmore4) {
          while (hasmore1 && hasmore2 && hasmore4) {
             if (comp.compare(src1, src2) < 0) {
                 if (comp.compare(src1, src4) < 0) {
                     hasmore1 = copy(src1, dest);
                 } else {
                     hasmore4 = copy(src4, dest);
                 }
             }
             else {
                 if (comp.compare(src2, src4) < 0) {
                     hasmore2 = copy(src2, dest);
                 } else {
                     hasmore4 = copy(src4, dest);
                 }
             }
          }
      } else if (hasmore1 && hasmore3 && hasmore4) {
          while (hasmore1 && hasmore3 && hasmore4) {
             if (comp.compare(src1, src3) < 0) {
                 if (comp.compare(src1, src4) < 0) {
                     hasmore1 = copy(src1, dest);
                 } else {
                     hasmore4 = copy(src4, dest);
                 }
             }
             else {
                 if (comp.compare(src3, src4) < 0) {
                     hasmore3 = copy(src3, dest);
                 } else {
                     hasmore4 = copy(src4, dest);
                 }
             }
          }
      } else if (hasmore2 && hasmore3 && hasmore4) {
          while (hasmore2 && hasmore3 && hasmore4) {
             if (comp.compare(src2, src3) < 0) {
                 if (comp.compare(src2, src4) < 0) {
                     hasmore2 = copy(src2, dest);
                 } else {
                     hasmore4 = copy(src4, dest);
                 }
             }
             else {
                 if (comp.compare(src3, src4) < 0) {
                     hasmore3 = copy(src3, dest);
                 } else {
                     hasmore4 = copy(src4, dest);
                 }
             }
          }
      }

      if (hasmore1 && hasmore2) {
          while (hasmore1 && hasmore2) {
              if (comp.compare(src1, src2) < 0) {
                  hasmore1 = copy(src1, dest);
              }
              else {
                  hasmore2 = copy(src2, dest);
              }
          }
      } else if (hasmore1 && hasmore3) {
          while (hasmore1 && hasmore3) {
              if (comp.compare(src1, src3) < 0) {
                  hasmore1 = copy(src1, dest);
              }
              else {
                  hasmore3 = copy(src3, dest);
              }
          }
      } else if (hasmore1 && hasmore4) {
          while (hasmore1 && hasmore4) {
              if (comp.compare(src1, src4) < 0) {
                  hasmore1 = copy(src1, dest);
              }
              else {
                  hasmore4 = copy(src4, dest);
              }
          }
      } else if (hasmore2 && hasmore3) {
          while (hasmore2 && hasmore3) {
              if (comp.compare(src2, src3) < 0) {
                  hasmore2 = copy(src2, dest);
              }
              else {
                  hasmore3 = copy(src3, dest);
              }
          }
      } else if (hasmore2 && hasmore4) {
          while (hasmore2 && hasmore4) {
              if (comp.compare(src2, src4) < 0) {
                  hasmore2 = copy(src2, dest);
              }
              else {
                  hasmore4 = copy(src4, dest);
              }
          }
      } else if (hasmore3 && hasmore4) {
          while (hasmore3 && hasmore4) {
              if (comp.compare(src3, src4) < 0) {
                  hasmore3 = copy(src3, dest);
              }
              else {
                  hasmore4 = copy(src4, dest);
              }
          }
      }

      if (hasmore1) {
          while (hasmore1) {
              hasmore1 = copy(src1, dest);
          }
      }
      else if (hasmore2) {
          while (hasmore2) {
              hasmore2 = copy(src2, dest);
          }
      } else if (hasmore3){
          while (hasmore3) {
              hasmore3 = copy(src3, dest);
          }
      } else {
          while (hasmore4) {
              hasmore4 = copy(src4, dest);
          }
      }

      src1.close();
      src2.close();
      src3.close();
      src4.close();
      dest.close();
      return result;
   }

   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }
}
