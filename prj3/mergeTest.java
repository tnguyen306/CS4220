import simpledb.query.*;
import simpledb.record.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.materialize.*;
import java.util.*;

public class mergeTest {
    public static void main(String[] args) {
        SimpleDB.init("studentdb");
        Transaction tx = new Transaction();
        Plan p1 = new TablePlan("messy", tx);

        // System.out.println("The initial content in the messy file");
        // TableScan testScan = (TableScan) p1.open();
        // while (testScan.next()) {
        //     System.out.println(testScan.getVal("col1"));
        // }
        // testScan.close();
        // System.out.println(" ");

        List<String> sf = Arrays.asList("col1");
        Schema sch = p1.schema();
        int nruns = 2;
        if (args.length > 0 ) {
            nruns = Integer.parseInt(args[0]);
        }
        if ((nruns > 4) || (nruns < 2)) {
            System.out.println("Bad Input -- Max # of sort runs is 4");
            tx.commit();
            return ;
        }
        System.out.println("Begin MERGE SORT Test With " + nruns + " Run Merging");
        System.out.println(" ");

        Plan p2 = new SortPlan(p1, sf, tx, nruns);
        Scan s2 = p2.open();

        System.out.println(" ");
        System.out.println("Sorted Result ");
        System.out.println(" ");

        while(s2.next()) {
            int col1 = s2.getInt("col1");
            System.out.println(" " + col1 );
        }
        s2.close();
        tx.commit();

        System.out.println("END MERGE SORT TEST");
    }
}
