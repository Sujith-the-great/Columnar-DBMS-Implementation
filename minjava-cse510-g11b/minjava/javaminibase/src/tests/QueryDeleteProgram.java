package tests;

import java.io.*;
import java.lang.*;
import java.util.*;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import index.*;
import bitmap.*;
import columnar.*;
import iterator.*;
import diskmgr.*;

public class QueryDeleteProgram {
    public static void main(String[] args) {
        try {
            // Parse Command Line
            if (args.length != 9) {
                System.out.println("Usage: query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE PURGEDB. Num of arguments passed: " + Integer.toString(args.length));
                //Print all arguments
                
            }

            // Parse Command Line
            String columnDBName = args[0]+".minibase-db";
            String columnarFileName = args[1];
            String[] targetColumnNames = args[2].substring(1,args[2].length()-1).split(",");
            
            String columnName = args[3].substring(1);
            String operator = args[4];
            String value = args[5].substring(0, args[5].length() - 1);

            //String valueConstraint = args[6];
            int numBuf = Integer.parseInt(args[6]);
            String accessType = args[7];
            Boolean purgeDB = Boolean.parseBoolean(args[8]);

            // Parse value constraints (in form of {columnName operator value})
            String[] valueConstraintsParts = new String[3];
            valueConstraintsParts[0] = columnName;
            valueConstraintsParts[1] = operator;
            valueConstraintsParts[2] = value;


            //Print all arguments
            /*System.out.println(columnDBName);
            System.out.println(columnarFileName);
            System.out.println(Arrays.toString(targetColumnNames));
            System.out.println(columnName+" "+operator+" "+value);
            System.out.println(numBuf);
            System.out.println(accessType);
            System.out.println(purgeDB);*/

            // Check if Database exists
            try {
                System.out.println("Open the DB");
                SystemDefs.MINIBASE_RESTART_FLAG = true;
                SystemDefs sysdef = new SystemDefs( columnDBName, 10000, GlobalConst.NUMBUF,"Clock"); 
            } catch (Exception e) {
                System.out.println("DB does not exists.");
                e.printStackTrace();
                return;
            }
            
            // Initialize Minibase Environment
            //BufMgr bufMgr = new BufMgr(numBuf, "Clock");
            //PageId pageId = SystemDefs.JavabaseDB.get_file_entry(columnarFileName+".hdr");        
            //Page page = new Page();
            //SystemDefs.JavabaseDB.read_page(pageId, page);
            //byte[] data = page.getpage();

            Columnarfile columnarfile = new Columnarfile(columnarFileName);


            // Perfrom Query Operation
            try {
                switch (accessType) {
                    case "FILESCAN":
                        executeFileScanQueryDelete(columnarfile, columnDBName, columnarFileName, targetColumnNames, valueConstraintsParts, purgeDB);
                        break;
                    case "COLUMNSCAN":
                        executeColumnScanQueryDelete(columnDBName, columnarFileName, targetColumnNames, valueConstraintsParts, purgeDB);
                        break;
                    case "BITMAP":
                        executeBitMapQueryDelete(columnDBName, columnarFileName, targetColumnNames, valueConstraintsParts, purgeDB);//, bufMgr);
                        break;
                    case "BTREE":
                        executeBTreeQueryDelete(columnDBName, columnarFileName, targetColumnNames, valueConstraintsParts, purgeDB);//, bufMgr);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // Monitor Disk I/O
            // Close Resources

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    private static void executeFileScanQueryDelete(Columnarfile columnarFile, String columnDBName, String columnarFileName, String[] targetColumns, String[] valueConstraints, Boolean purgeDB)
    {
        PCounter.initialize();
        try {
            String columnName = valueConstraints[0];
            String operator = valueConstraints[1];
            String value = valueConstraints[2]; 

            int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName)+1;
            

            // Columnar Attribute Types
            AttrType[] types = columnarFile.type;
                       
            // Prepare the output fields specification for the projection
            FldSpec[] Sprojection = new FldSpec[targetColumns.length];
            for (int i = 0; i < targetColumns.length; i++) {
                Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }  

            // Prepare string sizes
            short[] strSizes = columnarFile.strSizes;        
            
            // Query Condition Expression
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(operator);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = types[columnNumber-1];
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), columnNumber);
            expr[0].next = null;
            expr[1] = null;
            

            System.out.println(value);
            // Fidn the Type of the real value and set the operand2
            if (expr[0].type2.attrType == AttrType.attrString) {
                expr[0].operand2.string = value;
            } else {
                expr[0].operand2.integer = Integer.parseInt(value);
            }
            
            
            // Create the file scan
            ColumnarFileScan fileScan = new ColumnarFileScan(columnarFileName, types, strSizes, (short)columnarFile.numColumns, targetColumns.length, Sprojection, expr); 

            // Retrieve tuples that match the value constraint
            // Tuple tuple;
            System.out.println("Deleted Tuples: ");
            TID tid = null;
            while (((tid = fileScan.get_next_tid()) != null)) {
                // Print the tuple
                Tuple tuple = columnarFile.getTuple(tid);
                tuple.print(columnarFile.type);
                columnarFile.markTupleDeleted(tid);
                // System.out.println(tuple.toString());                
                
            }            
            // Close the file scan
            fileScan.close();            
            // Output the number of disk pages read and written
            if(purgeDB)
            {
                columnarFile.purgeAllDeletedTuples();
            
            }

            SystemDefs.JavabaseBM.flushAllPages();
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }  

    private static void executeColumnScanQueryDelete(String columnDBName, String columnarFileName, String[] targetColumns, String[] valueConstraints, Boolean purgeDB)
    {
        PCounter.initialize();
       try {
            String columnName = valueConstraints[0];

            // Create a ColumnarFile object
            Columnarfile columnarFile = new Columnarfile(columnarFileName);
            
            // Get the column number for the column page name
            int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName)+1;
            //String columnFileName = columnarFileName+"."+Integer.toString(columnNumber);
            
            // Create a ColumnarFileScan object
            Scan scan = columnarFile.openColumnScan(columnNumber); 

            // Open File to get PageID of the column page
            //PageId pageId = SystemDefs.JavabaseDB.get_file_entry(columnFileName);

            // Get RID for first record in the page
            RID rid = new RID();

            System.out.println("Deleted Tuples: ");
            // Retrieve tuples that match the value constraint
            TID tid=null;
            while ((tid = new TID(columnarFile.numColumns, columnarFile.getPositionFromRid(rid, columnNumber))) != null){//(tuple = scan.getNext(rid)) != null) {
                // Print the tuple
                //System.out.println(tuple.toString());
                Tuple tuple = columnarFile.getTuple(tid);
                byte[] data = tuple.getTupleByteArray();
                switch (columnarFile.type[columnNumber-1].attrType) {
                case AttrType.attrInteger:
                    System.out.println(Convert.getIntValue(0, data));
                    break;
                case AttrType.attrString:
                    System.out.println(Convert.getStrValue(0, data, data.length));
                    break;
                case AttrType.attrReal:
                    System.out.println(Convert.getFloValue(0, data));
                    break;
                }
                columnarFile.markTupleDeleted(tid);
            }

            if(purgeDB)
            {
                    columnarFile.purgeAllDeletedTuples();
            
            }
            SystemDefs.JavabaseBM.flushAllPages();

            // Print the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());

            // Close the file scan
            scan.closescan();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void executeBTreeQueryDelete(String columnDBName, String columnarFileName, String[] targetColumns, String[] valueConstraints, Boolean purgeDB)//, BufMgr bufMgr )
    {   
        PCounter.initialize();
        // Create a BTreeFile and get the header heapfile for the columnar file
        try{
            // Parse value constraints
            String columnName = valueConstraints[0];
            String operator = valueConstraints[1];
            String value = valueConstraints[2]; 
            boolean isStringKey = false;

            
            Columnarfile columnarFile = new Columnarfile(columnarFileName);
            int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName)+1;

            if(!columnarFile.createBTreeIndex(columnNumber)){
                System.out.println("BTree Index for column " + columnName + " already exists");
            }

            BTreeFile btf = new BTreeFile(columnarFileName + ".btree" + Integer.toString(columnNumber));

            // Creates a BTreeFile object using the index column name
            IntegerKey intKey = null;
            StringKey strKey = null;
            
            //If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
            if (columnarFile.type[columnNumber-1].attrType == AttrType.attrInteger) {
                intKey = new IntegerKey(Integer.parseInt(value));
            } else if (columnarFile.type[columnNumber-1].attrType == AttrType.attrString) {
                // Create a key for the search value
                isStringKey = true;
                strKey = new StringKey(value);
            } 

            System.out.println("Deleted Tuples: ");
            // Create a BTreeFileScan object
            BTFileScan btfScan = null;
            //if operator = range, use the range scan method
            if (operator.equals("RANGE")) {
                btfScan = btf.new_scan(null, null);
            }
            // If operator is less than or equal to, use the range scan method
            else if (operator.equals("<=")) {
                if (isStringKey) {
                    btfScan = btf.new_scan(null, strKey);
                } else {
                    btfScan = btf.new_scan(null, intKey);
                }
            } // If operator is greater than or equal to, use the range scan method
            else if (operator.equals(">=")) {
                if (isStringKey) {
                    btfScan = btf.new_scan(strKey, null);
                } else {
                    btfScan = btf.new_scan(intKey, null);
                }
            } // If operator is Not equal to, use the range scan method
            else if (operator.equals("!=") || operator.equals("NOT")) {
                if (isStringKey) {
                    btfScan = btf.new_scan(null, strKey);
                    KeyDataEntry entry;
                    while ((entry = btfScan.get_next()) != null) {
                        // Print the tuple
                        int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
                        TID tid1 = columnarFile.getTidFromPosition(pos);
                        
                        Tuple tuple = columnarFile.getTuple(tid1);
                        tuple.print(columnarFile.type);
                        columnarFile.markTupleDeleted(tid1);
                    }
                    btfScan.DestroyBTreeFileScan();
                    btfScan = btf.new_scan(strKey, null);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)-1);
                    btfScan = btf.new_scan(null, intKey);
                    KeyDataEntry entry;
                    while ((entry = btfScan.get_next()) != null) {
                        // Print the tuple
                        int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
                        TID tid1 = columnarFile.getTidFromPosition(pos);
                        
                        Tuple tuple = columnarFile.getTuple(tid1);
                        tuple.print(columnarFile.type);
                        columnarFile.markTupleDeleted(tid1);
                    }
                    btfScan.DestroyBTreeFileScan();
                    intKey = new IntegerKey(Integer.parseInt(value)+1);
                    btfScan = btf.new_scan(intKey, null);
                }
            } else if (operator.equals(">")){
                if (isStringKey) {
                    //Figure out how to increment string key
                    btfScan = btf.new_scan(strKey, null);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)+1);
                    btfScan = btf.new_scan(intKey, null);
                }
            } else if (operator.equals("<")){
                if (isStringKey) {
                    btfScan = btf.new_scan(null, strKey);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)-1);
                    btfScan = btf.new_scan(null, intKey);
                }
            }
            // If operator is equal to, use the point scan method 
            else {
                // Create a point scan
                if (isStringKey) {
                    btfScan = btf.new_scan(strKey, strKey);
                } else {
                    btfScan = btf.new_scan(intKey, intKey);
                }
            }

            // Iterate through B-Tree index entires
            KeyDataEntry entry;
            while ((entry = btfScan.get_next()) != null) {
                // Print the tuple
                int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
                TID tid1 = columnarFile.getTidFromPosition(pos);
                
                Tuple tuple = columnarFile.getTuple(tid1);
                tuple.print(columnarFile.type);
                columnarFile.markTupleDeleted(tid1);
            }

            if(purgeDB)
            {
                columnarFile.purgeAllDeletedTuples();
            
            }
            SystemDefs.JavabaseBM.flushAllPages();
            // Print the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());

            // Close the BTreeFileScan
            btfScan.DestroyBTreeFileScan();
            btf.close();

        } catch (Exception e){
            e.printStackTrace();
        } 

    }    
    
    private static void executeBitMapQueryDelete(String columnDBName, String columnarFileName, String[] targetColumns, String[] valueConstraints, Boolean purgeDB)//, BufMgr bufMgr )
    {
        try {
            // Parse value constraints
            String columnName = valueConstraints[0];
            String operator = valueConstraints[1];
            String value = valueConstraints[2]; 
            boolean isStringKey = false;

            // Create a ColumnarFile object
            Columnarfile columnarFile = new Columnarfile(columnarFileName);
            int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName);

            TID tid = null;
            ValueClass valueClass = columnarFile.getValue(tid, columnNumber);

            if(!columnarFile.createBitMapIndex(columnNumber, valueClass))
            {
                System.out.println("BitMap Index for column " + columnName + " already exists");
            }

            BitMapFile bitMap = new BitMapFile(columnarFileName + ".bitMap" + Integer.toString(columnNumber));//, columnarFile, columnNumber, valueClass);

            
            IntegerKey intKey = null;
            StringKey strKey = null;
            
            //If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
            if (columnarFile.type[columnNumber].attrType == AttrType.attrInteger) {
                intKey = new IntegerKey(Integer.parseInt(value));
            } else if (columnarFile.type[columnNumber].attrType == AttrType.attrString) {
                // Create a key for the search value
                isStringKey = true;
                strKey = new StringKey(value);
            } 

            // Create a BitMapFileScan object
            BMFileScan bitMapScan = null; 

            //if operator = range, use the range scan method
            if (operator.equals("RANGE")) {
                bitMapScan = bitMap.new_scan(null, null);
            }
            // If operator is less than or equal to, use the range scan method
            else if (operator.equals("<=")) {
                if (isStringKey) {
                    bitMapScan = bitMap.new_scan(null, strKey);
                } else {
                    bitMapScan = bitMap.new_scan(null, intKey);
                }
            } // If operator is greater than or equal to, use the range scan method
            else if (operator.equals(">=")) {
                if (isStringKey) {
                    bitMapScan = bitMap.new_scan(strKey, null);
                } else {
                    bitMapScan = bitMap.new_scan(intKey, null);
                }
            } // If operator is Not equal to, use the range scan method
            else if (operator.equals("!=") || operator.equals("NOT")) {
                if (isStringKey) {
                    bitMapScan = bitMap.new_scan(null, strKey);
                    KeyDataEntry entry;
                    while ((entry = bitMapScan.get_next()) != null) {
                        // Print the tuple
                        System.out.println(entry.data.toString());
                    }
                    //bitMapScan.Destroy();
                    bitMapScan = bitMap.new_scan(strKey, null);
                } else {
                    bitMapScan = bitMap.new_scan(null, intKey);
                    KeyDataEntry entry;
                    while ((entry = bitMapScan.get_next()) != null) {
                        // Print the tuple
                        System.out.println(entry.data.toString());
                    }
                    //bitMapScan.Destroy();
                    bitMapScan = bitMap.new_scan(intKey, null);
                }
            } 
            // If operator is equal to, use the point scan method 
            else {
                // Create a point scan
                if (isStringKey) {
                    bitMapScan = bitMap.new_scan(strKey, strKey);
                } else {
                    bitMapScan = bitMap.new_scan(intKey, intKey);
                }
            }

            // Print the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
