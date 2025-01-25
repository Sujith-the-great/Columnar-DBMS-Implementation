/*
 * File - CFTest.java
 *
 * Original Author - Vivian Roshan Adithan
 *
 * Description - 
 *		...
 */
package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import chainexception.*;
import columnar.*;
import heap.*;
import index.ColumnarIndexScan;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

class DummyRecord {

  // content of the record
  public int ival;
  public float fval;
  public String name;

  // length under control
  private int reclen;

  private byte[] data;

  /**
   * Default constructor
   */
  public DummyRecord() {
  }

  /**
   * another constructor
   */
  public DummyRecord(int _reclen) {
    setRecLen(_reclen);
    data = new byte[_reclen];
  }

  /**
   * constructor: convert a byte array to DummyRecord object.
   * 
   * @param arecord a byte array which represents the DummyRecord object
   */
  public DummyRecord(byte[] arecord)
      throws java.io.IOException {
    setIntRec(arecord);
    setFloRec(arecord);
    setStrRec(arecord);
    data = arecord;
    setRecLen(name.length());
  }

  /**
   * constructor: translate a tuple to a DummyRecord object
   * it will make a copy of the data in the tuple
   * 
   * @param atuple: the input tuple
   */
  public DummyRecord(Tuple _atuple)
      throws java.io.IOException {
    data = new byte[_atuple.getLength()];
    data = _atuple.getTupleByteArray();
    setRecLen(_atuple.getLength());

    setIntRec(data);
    setFloRec(data);
    setStrRec(data);

  }

  /**
   * convert this class objcet to a byte array
   * this is used when you want to write this object to a byte array
   */
  public byte[] toByteArray()
      throws java.io.IOException {
    // data = new byte[reclen];
    Convert.setIntValue(ival, 0, data);
    Convert.setFloValue(fval, 4, data);
    Convert.setStrValue(name, 8, data);
    return data;
  }

  /**
   * get the integer value out of the byte array and set it to
   * the int value of the DummyRecord object
   */
  public void setIntRec(byte[] _data)
      throws java.io.IOException {
    ival = Convert.getIntValue(0, _data);
  }

  /**
   * get the float value out of the byte array and set it to
   * the float value of the DummyRecord object
   */
  public void setFloRec(byte[] _data)
      throws java.io.IOException {
    fval = Convert.getFloValue(4, _data);
  }

  /**
   * get the String value out of the byte array and set it to
   * the float value of the HTDummyRecorHT object
   */
  public void setStrRec(byte[] _data)
      throws java.io.IOException {
    // System.out.println("reclne= "+reclen);
    // System.out.println("data size "+_data.size());
    name = Convert.getStrValue(8, _data, reclen - 8);
  }

  // Other access methods to the size of the String field and
  // the size of the record
  public void setRecLen(int size) {
    reclen = size;
  }

  public int getRecLength() {
    return reclen;
  }
}

/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

class CFDriver extends TestDriver implements GlobalConst {

  private final static boolean OK = true;
  private final static boolean FAIL = false;

  private int choice;
  private final static int reclen = 32;

  public CFDriver() {
    super("cftest");
    // choice = 1; // big enough for file to occupy > 1 data page
    // choice = 100; // big enough for file to occupy > 1 data page
    // choice = 2000; // big enough for file to occupy > 1 directory page
    choice = 10;
  }

  public boolean runTests() {

    System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

    SystemDefs sysdef = new SystemDefs(dbpath, 200, 100, "Clock");

    // Kill anything that might be hanging around
    String newdbpath;
    String newlogpath;
    String remove_logcmd;
    String remove_dbcmd;
    String remove_cmd = "/bin/rm -rf ";

    newdbpath = dbpath;
    newlogpath = logpath;

    remove_logcmd = remove_cmd + logpath;
    remove_dbcmd = remove_cmd + dbpath;

    // Commands here is very machine dependent. We assume
    // user are on UNIX system here
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("IO error: " + e);
    }

    remove_logcmd = remove_cmd + newlogpath;
    remove_dbcmd = remove_cmd + newdbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("IO error: " + e);
    }

    // Run the tests. Return type different from C++
    boolean _pass = runAllTests();

    // Clean up again
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("IO error: " + e);
    }

    System.out.print("\n" + "..." + testName() + " tests ");
    System.out.print(_pass == OK ? "completely successfully" : "failed");
    System.out.print(".\n\n");

    return _pass;
  }

  protected boolean test1() {

    // reset read/write for test case
    PCounter.initialize();

    System.out.println("\n  ------------------Test 1: TupleScan Test\n");
    boolean status = OK;
    TID tid = null;
    Columnarfile f = null;

    AttrType[] attrType = new AttrType[3];
    attrType[0] = new AttrType(AttrType.attrInteger);
    attrType[1] = new AttrType(AttrType.attrReal);
    attrType[2] = new AttrType(AttrType.attrString);

    short[] Ssizes = new short[1];
    Ssizes[0] = 30; // first elt. is 30

    String[] columnNames = new String[3];
    columnNames[0] = "column1:int";
    columnNames[1] = "column2:float";
    columnNames[2] = "column3:string";

    System.out.println("  - Create a Columnar file\n");
    try {
      f = new Columnarfile("test1", 3, attrType, Ssizes, columnNames);
      f = new Columnarfile("test1");
      System.out.println("numColumns: " + Columnarfile.numColumns);
      for (int i = 0; i < Columnarfile.numColumns; i++) {
        System.out.println("AttrType: " + f.type[i].attrType);
      }
      System.out.println("String Size length: " + f.strSizes.length);
      for (int i = 0; i < f.strSizes.length; i++) {
        System.out.println("String Size: " + f.strSizes[i]);
      }
      System.out.println("Column Names length: " + f.columnNames.length);
      for (int i = 0; i < f.columnNames.length; i++) {
        System.out.println("Column Name: " + f.columnNames[i]);
      }
      System.out.println("getTupleCnt: " + f.getTupleCnt());
    } catch (Exception e) {
      System.err.println("*** Could not create heap file\n");
      e.printStackTrace();
    }

    System.out.println("  - Add " + choice + " records to the file\n");
    for (int i = 0; (i < choice) && (status == OK); i++) {
      // fixed length record
      DummyRecord rec = new DummyRecord(reclen);
      rec.ival = i;
      rec.fval = (float) (i * 2.5);
      rec.name = "record" + i;
      // Ssizes[0] = (short) rec.name.length();
      Tuple t = new Tuple();

      try {
        t.setHdr((short) 3, attrType, Ssizes);
        t.setIntFld(1, rec.ival);
        t.setFloFld(2, rec.fval);
        t.setStrFld(3, rec.name);
        tid = f.insertTuple(t.getTupleByteArray());
      } catch (Exception e) {
        status = FAIL;
        System.err.println("*** Error inserting record " + i + "\n");
        e.printStackTrace();
      }
      try {
        t = f.getTuple(tid);
        t.print(attrType);
      } catch (Exception e) {
        status = FAIL;
        System.err.println("*** Error getting record " + i + "\n");
        e.printStackTrace();
      }
    }
    try {
      System.out.println("Tuple count: " + f.getTupleCnt());
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** Error getting tuple count\n");
      e.printStackTrace();
    }

    int positionTIDCount = 0;
    TID positionTID = null;

    try {
      TupleScan tupleScan = f.openTupleScan();
      System.out.println("Tuple scan getNext:");
      Tuple t = tupleScan.getNext(tid);
      while (t != null) {
        t.print(attrType);
        if (positionTIDCount == 2)
          positionTID = new TID(tid.numRIDs, tid.position, tid.recordIDs);
        if (positionTIDCount % 3 == 0)
          f.markTupleDeleted(tid);
        t = tupleScan.getNext(tid);
        positionTIDCount++;
      }
      tupleScan.closetuplescan();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** TupleScan Error\n");
      e.printStackTrace();
    }

    try {
      TupleScan tupleScan = f.openTupleScan();
      System.out.println("Tuple scan position:");
      boolean isPositionChanged = tupleScan.position(positionTID);
      System.out.println("Is position changed: " + isPositionChanged);
      tupleScan.closetuplescan();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** TupleScan Error\n");
      e.printStackTrace();
    }

    try {
      IntegerValueClass intValueClass = (IntegerValueClass) f.getValue(positionTID, 1);
      FloatValueClass floValueClass = (FloatValueClass) f.getValue(positionTID, 2);
      StringValueClass strValueClass = (StringValueClass) f.getValue(positionTID, 3);
      System.out.println("Value at positionTID: " + intValueClass.classValue + " " + floValueClass.classValue + " "
          + strValueClass.classValue + " " + strValueClass.classValue.length());
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** getValue\n");
      e.printStackTrace();
    }

    try {
      System.out.println("purgeAllDeletedTuples");
      f.markTupleDeleted(f.getTidFromPosition(1));
      f.purgeAllDeletedTuples();
      TupleScan tupleScan = f.openTupleScan();
      System.out.println("Tuple scan getNext:");
      Tuple t = tupleScan.getNext(tid);
      while (t != null) {
        t.print(attrType);
        t = tupleScan.getNext(tid);
      }
      tupleScan.closetuplescan();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** purgeDelete\n");
      e.printStackTrace();
    }

    try {
      System.out.println("Tuple count: " + f.getTupleCnt());
      Tuple t = new Tuple();
      t.setHdr((short) 3, attrType, Ssizes);
      t.setIntFld(1, 100);
      t.setFloFld(2, 100.0f);
      t.setStrFld(3, "record0");
      tid = f.insertTuple(t.getTupleByteArray());
      System.out.println("Tuple count: " + f.getTupleCnt());
      TupleScan tupleScan = f.openTupleScan();
      System.out.println("Tuple scan position:");
      boolean isPositionChanged = tupleScan.position(positionTID);
      System.out.println("Is position changed: " + isPositionChanged);
      tupleScan.closetuplescan();
      tupleScan = f.openTupleScan();
      t = tupleScan.getNext(tid);
      while (t != null) {
        t.print(attrType);
        t = tupleScan.getNext(tid);
      }
      tupleScan.closetuplescan();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** TupleScan Error\n");
      e.printStackTrace();
    }

    try {
      IntegerValueClass intValueClass = (IntegerValueClass) f.getValue(positionTID, 1);
      FloatValueClass floValueClass = (FloatValueClass) f.getValue(positionTID, 2);
      StringValueClass strValueClass = (StringValueClass) f.getValue(positionTID, 3);
      System.out.println("Value at positionTID: " + intValueClass.classValue + " " + floValueClass.classValue + " "
          + strValueClass.classValue + " " + strValueClass.classValue.length());
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** getValue\n");
      e.printStackTrace();
    }

    try {
      System.out.println("Position RID");
      Tuple pt = f.getTuple(positionTID);
      pt.print(attrType);
      int position = f.getPositionFromRid(positionTID.recordIDs[0], 1);
      System.out.println("Position from rid: " + position);
      RID rid = f.getRidFromPosition(position, 1);
      System.out.println("RID from position: " + rid.pageNo.pid + " " + rid.slotNo);
      TID newtid = f.getTidFromPosition(position);
      Tuple nt = f.getTuple(newtid);
      nt.print(attrType);
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** Error deleting and recreating file\n");
      e.printStackTrace();
    }

    FldSpec[] projlist = new FldSpec[3];
    RelSpec rel = new RelSpec(RelSpec.outer);
    projlist[0] = new FldSpec(rel, 1);
    projlist[1] = new FldSpec(rel, 2);
    projlist[2] = new FldSpec(rel, 3);

    CondExpr[] expr = new CondExpr[2];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrInteger);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
    expr[0].operand2.integer = 1;
    expr[0].next = null;
    expr[1] = null;

    try {
      System.out.println("ColumnarFileScan");
      ColumnarFileScan scan = new ColumnarFileScan("test1", attrType, Ssizes, (short) 3, 3, projlist, null);
      Tuple t = scan.get_next();
      while (t != null) {
        t.print(attrType);
        t = scan.get_next();
      }
      scan.close();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** ColumnarFileScan\n");
      e.printStackTrace();
    }

    try {
      System.out.println("ColumnarIndexScan: B_Tree");
      f.createBTreeIndex(1);
      f.createBTreeIndex(3);
      ColumnarIndexScan iscan;
      String[] indNames = new String[3];
      indNames[0] = "test1.btree1";
      indNames[1] = "test1.btree2";
      indNames[2] = "test1.btree3";
      expr = new CondExpr[2];
      expr[0] = new CondExpr();
      expr[0].op = new AttrOperator(AttrOperator.aopGE);
      expr[0].type1 = new AttrType(AttrType.attrSymbol);
      expr[0].type2 = new AttrType(AttrType.attrInteger);
      expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
      expr[0].operand2.integer = 3;
      expr[0].next = null;
      expr[1] = null;
      int[] fldNum = new int[1];
      fldNum[0] = 1;
      FldSpec[] projlist2 = new FldSpec[2];
      projlist2[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
      projlist2[1] = new FldSpec(new RelSpec(RelSpec.outer), 3);
      System.out.println("projlist2 length: " + projlist2.length);
      AttrType[] attrType2 = new AttrType[projlist2.length];
      for (int i = 0; i < projlist2.length; i++) {
        attrType2[i] = attrType[projlist2[i].offset - 1];
        System.out.println("AttrType: " + attrType2[i].attrType);
      }
      iscan = new ColumnarIndexScan("test1", fldNum, new IndexType(IndexType.B_Index), indNames, attrType, Ssizes, 3, 2,
          projlist2, expr, false);
      Tuple t = iscan.get_next();
      while (t != null) {
        t.print(attrType2);
        t = iscan.get_next();
      }
      iscan.close();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** ColumnarIndexScan: B_Tree\n");
      e.printStackTrace();
    }

    try {
      System.out.println("ColumnarIndexScan: BitMap");
      f.createBitMapIndex(1, new IntegerValueClass());
      f.createBitMapIndex(3, new StringValueClass());
      ColumnarIndexScan iscan;
      String[] indNames = new String[3];
      indNames[0] = "test1.bitmap1";
      indNames[1] = "test1.bitmap2";
      indNames[2] = "test1.bitmap3";
      expr = new CondExpr[2];
      expr[0] = new CondExpr();
      expr[0].op = new AttrOperator(AttrOperator.aopGE);
      expr[0].type1 = new AttrType(AttrType.attrSymbol);
      expr[0].type2 = new AttrType(AttrType.attrInteger);
      expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
      expr[0].operand2.integer = 3;
      expr[0].next = null;
      expr[1] = null;
      int[] fldNum = new int[1];
      fldNum[0] = 1;
      iscan = new ColumnarIndexScan("test1", fldNum, new IndexType(IndexType.Bitmap), indNames, attrType, Ssizes, 3, 3,
          projlist, expr, false);
      Tuple t = iscan.get_next();
      while (t != null) {
        t.print(attrType);
        t = iscan.get_next();
      }
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** ColumnarIndexScan: Bitmap\n");
      e.printStackTrace();
    }

    try {
      f.deleteColumnarFile();
      f = new Columnarfile("test1", 3, attrType, Ssizes, columnNames);
      System.out.println("Tuple count: " + f.getTupleCnt());
      f.deleteColumnarFile();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** Error deleting and recreating file\n");
      e.printStackTrace();
    }

    try {
      SystemDefs.JavabaseBM.flushAllPages();
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("PCounter.getReadCount: " + PCounter.getReadCount()
        + " PCounter.getWriteCount: " + PCounter.getWriteCount());
    return status;
  }

  protected boolean runAllTests() {

    boolean _passAll = OK;

    if (!test1()) { _passAll = FAIL; }
    // if (!test2()) { _passAll = FAIL; }
    // if (!test3()) { _passAll = FAIL; }
    // if (!test4()) { _passAll = FAIL; }
    // if (!test5()) { _passAll = FAIL; }
    // if (!test6()) { _passAll = FAIL; }

    return _passAll;
  }

  protected String testName() {
    return "Columnar File";
  }
}

public class CFTest {

  public static void main(String argv[]) {

    CFDriver cd = new CFDriver();
    boolean dbstatus;

    dbstatus = cd.runTests();

    if (dbstatus != true) {
      System.err.println("Error encountered during columnar file tests:\n");
      Runtime.getRuntime().exit(1);
    }

    Runtime.getRuntime().exit(0);
  }
}
