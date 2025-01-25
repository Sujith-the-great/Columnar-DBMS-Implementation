/*
 * File - Columnarfile.java
 *
 * Original Author - Vivian Roshan Adithan
 *
 * Description - 
 *		...
 */
package columnar;

import java.io.IOException;
import bitmap.*;
import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.*;
import global.*;
import heap.*;

public class Columnarfile implements GlobalConst {
  public static int numColumns;
  public AttrType[] type;
  public short[] strSizes;
  public String[] columnNames;

  private String _fileName;
  private boolean _file_deleted;

  public String get_fileName() {
    return _fileName;
  }

  public String[] getColumnName() {
    return columnNames;
  }

  public AttrType[] getColumnTypes() {
    return type;
  }

  public Columnarfile(String name)
      throws FileIOException,
      InvalidPageNumberException,
      DiskMgrException,
      CFException,
      IOException,
      InvalidTupleSizeException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException {
    if (SystemDefs.JavabaseDB.get_file_entry(name + ".hdr") == null) {
      throw new CFException(null, "file does not exist");
    }
    this._fileName = name;
    Heapfile hdr = new Heapfile(name + ".hdr");
    RID rid = new RID();
    Scan scan = hdr.openScan();
    try {
      Tuple tuple = scan.getNext(rid);
      if (tuple == null) {
        throw new CFException(null, "file corrupted");
      }
      byte[] data = tuple.getTupleByteArray();
      Columnarfile.numColumns = Convert.getShortValue(0, data);
      this.type = new AttrType[Columnarfile.numColumns];
      for (int i = 0; i < Columnarfile.numColumns; i++) {
        this.type[i] = new AttrType(Convert.getIntValue(2 + 4 * i, data));
      }
      tuple = scan.getNext(rid);
      if (tuple == null) {
        throw new CFException(null, "file corrupted");
      }
      data = tuple.getTupleByteArray();
      short strSizesLength = Convert.getShortValue(0, data);
      if (strSizesLength == 0) {
        this.strSizes = null;
      } else {
        this.strSizes = new short[strSizesLength];
      }
      for (int i = 0; i < strSizesLength; i++) {
        this.strSizes[i] = Convert.getShortValue(2 + 2 * i, data);
      }
      tuple = scan.getNext(rid);
      if (tuple == null) {
        throw new CFException(null, "file corrupted");
      }
      data = tuple.getTupleByteArray();
      short columnNamesLength = Convert.getShortValue(0, data);
      if (columnNamesLength != Columnarfile.numColumns) {
        throw new CFException(null, "file corrupted");
      }
      this.columnNames = new String[columnNamesLength];
      for (int i = 0; i < Columnarfile.numColumns; i++) {
        this.columnNames[i] = Convert.getStrValue(2 + 50 * i, data, 50);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      scan.closescan();
    }
  }

  /*
   * Constructor summary
   * Columnarfile(java.lang.String name, int numColumns, AttrType[] type)
   * Initialize: if columnar file does not exits, create one
   * heapfile (‘‘name.columnid’’) per column; also create a
   * ‘‘name.hdr’’ file that contains relevant metadata.
   */
  public Columnarfile(String name, int numColumns, AttrType[] type,
      short[] sSizes, String[] colNames)
      throws HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException,
      InvalidSlotNumberException,
      InvalidTupleSizeException,
      SpaceNotAvailableException,
      CFException,
      FileIOException,
      InvalidPageNumberException,
      DiskMgrException {
    if (SystemDefs.JavabaseDB.get_file_entry(name + ".hdr") != null) {
      throw new CFException(null, "file alread exists");
    }
    _file_deleted = true;
    Columnarfile.numColumns = numColumns;
    this.type = type;
    this._fileName = name;
    this.strSizes = sSizes;
    this.columnNames = colNames;
    Heapfile hdr = new Heapfile(name + ".hdr");
    byte[] data = new byte[2 + 4 * numColumns];
    Convert.setShortValue((short) Columnarfile.numColumns, 0, data);
    for (int i = 0; i < Columnarfile.numColumns; i++) {
      Convert.setIntValue(this.type[i].attrType, 2 + 4 * i, data);
    }
    hdr.insertRecord(data);
    data = new byte[2 + 2 * numColumns];
    if (sSizes == null) {
      data = new byte[2];
      Convert.setShortValue((short) 0, 0, data);
    } else {
      Convert.setShortValue((short) sSizes.length, 0, data);
      for (int i = 0; i < sSizes.length; i++) {
        Convert.setShortValue(sSizes[i], 2 + 2 * i, data);
      }
    }
    hdr.insertRecord(data);
    data = new byte[2 + 50 * Columnarfile.numColumns];
    Convert.setShortValue((short) Columnarfile.numColumns, 0, data);
    for (int i = 0; i < numColumns; i++) {
      Convert.setStrValue(this.columnNames[i], 2 + 50 * i, data);
    }
    hdr.insertRecord(data);
    for (int i = 0; i < numColumns; i++) {
      new Heapfile(name + "." + Integer.toString(i + 1));
    }
    new Heapfile(name + ".deleted");
    _file_deleted = false;
  }

  // Delete all relevant files from the database.
  public void deleteColumnarFile()
      throws FileAlreadyDeletedException,
      CFException {
    if (_file_deleted)
      throw new FileAlreadyDeletedException(null, "file alread deleted");
    boolean isCFException = false;
    try {
      Heapfile hdr = new Heapfile(_fileName + ".hdr");
      hdr.deleteFile();
    } catch (Exception e) {
      isCFException = true;
      System.err.println("FileName :" + _fileName + ".hdr"
          + " deleteColumnarFile: " + e);
    }
    for (int i = 0; i < numColumns; i++) {
      try {
        Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(i));
        hf.deleteFile();
      } catch (Exception e) {
        isCFException = true;
        System.err.println("FileName :" + _fileName + "." + Integer.toString(i)
            + " deleteColumnarFile: " + e);
      }
    }
    try {
      Heapfile deleted = new Heapfile(_fileName + ".deleted");
      deleted.deleteFile();
    } catch (Exception e) {
      isCFException = true;
      System.err.println("FileName :" + _fileName + ".deleted"
          + " deleteColumnarFile: " + e);
    }
    if (isCFException) {
      throw new CFException(null, "deleteColumnarFile failed");
    }
  }

  // Insert tuple into file, return its tid
  public TID insertTuple(byte[] tuplePtr)
      throws CFException,
      InvalidSlotNumberException,
      InvalidTupleSizeException,
      SpaceNotAvailableException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException {
    int position = -1;
    RID[] recordIDs = new RID[numColumns];
    short fldCnt = Convert.getShortValue(0, tuplePtr);
    if (numColumns != fldCnt) {
      throw new CFException(null, "Number of columns in tuple != Columnarfile");
    }
    short[] fldOffset = new short[fldCnt + 1];
    for (int i = 0; i < fldCnt + 1; i++) {
      fldOffset[i] = Convert.getShortValue(2 * (i + 1), tuplePtr);
    }
    for (int i = 0; i < fldCnt; i++) {
      int length = fldOffset[i + 1] - fldOffset[i];
      byte[] data = new byte[length];
      System.arraycopy(tuplePtr, fldOffset[i], data, 0, length);
      Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(i + 1));
      Heapfile.RIDPosition ridPosition = hf.insertRecordRaw(data);
      recordIDs[i] = ridPosition.rid;
      if (position != -1 && position != ridPosition.position) {
        throw new CFException(null, "Insertion failed");
      } else {
        position = ridPosition.position;
      }
    }
    return new TID(numColumns, position, recordIDs);
  }

  // Read the tuple with the given tid from the columnar file
  public Tuple getTuple(TID tid)
      throws IOException,
      Exception {
    byte[] data = new byte[MINIBASE_PAGESIZE];
    Convert.setShortValue((short) numColumns, 0, data);
    short[] fldOffset = new short[numColumns + 1];
    fldOffset[0] = (short) (2 * (numColumns + 2));
    Convert.setShortValue(fldOffset[0], 2 * (0 + 1), data);
    for (int i = 0; i < numColumns; i++) {
      Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(i + 1));
      byte[] colData = hf.getRecord(tid.recordIDs[i]).getTupleByteArray();
      int leng = colData.length;
      fldOffset[i + 1] = (short) (fldOffset[i] + leng);
      Convert.setShortValue(fldOffset[i + 1], 2 * ((i + 1) + 1), data);
      System.arraycopy(colData, 0, data, fldOffset[i], leng);
    }
    Convert.setShortValue(fldOffset[numColumns], 2 * (numColumns + 1), data);
    byte[] tupleData = new byte[fldOffset[numColumns]];
    System.arraycopy(data, 0, tupleData, 0, fldOffset[numColumns]);
    Tuple t = new Tuple(tupleData, 0, fldOffset[numColumns]);
    t.setFldOffset(fldOffset);
    return t;
  }

  // Read the value with the given column and tid from the columnar file
  public ValueClass getValue(TID tid, int column)
      throws InvalidSlotNumberException,
      InvalidTupleSizeException,
      Exception {
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(column));
    Tuple t = hf.getRecord(tid.recordIDs[column - 1]);
    if (t == null) {
      throw new CFException(null, "Invalid TID");
    }
    byte[] data = t.getTupleByteArray();
    switch (type[column - 1].attrType) {
      case AttrType.attrInteger:
        return new IntegerValueClass(Convert.getIntValue(0, data));
      case AttrType.attrString:
        return new StringValueClass(Convert.getStrValue(0, data, data.length));
      case AttrType.attrReal:
        return new FloatValueClass(Convert.getFloValue(0, data));
      default:
        throw new CFException(null, "Invalid attribute type");
    }
  }

  // Return the number of tuples in the columnar file.
  public int getTupleCnt()
      throws HFException,
      InvalidSlotNumberException,
      InvalidTupleSizeException,
      HFDiskMgrException,
      HFBufMgrException,
      IOException {
    return new Heapfile(_fileName + ".1").getRecCnt();
  }

  // Initiate a sequential scan of tuples.
  public TupleScan openTupleScan()
      throws HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException,
      InvalidTupleSizeException {
    return new TupleScan(this);
  }

  // Initiate a sequential scan along a given column.
  public Scan openColumnScan(int columnNo)
      throws HFException,
      InvalidTupleSizeException,
      IOException,
      HFBufMgrException,
      HFDiskMgrException {
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(columnNo));
    return hf.openScan();
  }

  // Updates the specified record in the columnar file.
  public boolean updateTuple(TID tid, Tuple newtuple) {
    boolean status = true;
    for (int i = 0; i < numColumns; i++) {
      try {
        updateColumnofTuple(tid, newtuple, i + 1);
      } catch (Exception e) {
        System.err.println("updateTuple: " + e);
        status = false;
      }
    }
    return status;
  }

  // Updates the specified column of the specified record in the columnar file
  public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column)
      throws CFException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException,
      InvalidSlotNumberException,
      InvalidUpdateException,
      InvalidTupleSizeException,
      Exception {
    byte[] newTuplePtr = newtuple.getTupleByteArray();
    short fldCnt = Convert.getShortValue(0, newTuplePtr);
    if (numColumns != fldCnt) {
      throw new CFException(null, "Number of columns in tuple != Columnarfile");
    }
    if (column > numColumns) {
      throw new CFException(null, "Column number out of range");
    }
    short[] fldOffset = new short[fldCnt + 1];
    for (int i = 0; i < fldCnt + 1; i++) {
      fldOffset[i] = Convert.getShortValue(2 * (i + 1), newTuplePtr);
    }
    int length = fldOffset[column + 1] - fldOffset[column];
    byte[] data = new byte[length];
    System.arraycopy(newTuplePtr, fldOffset[column], data, 0, length);
    Tuple newColTuple = new Tuple(data, 0, length);
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(column));
    return hf.updateRecord(tid.recordIDs[column], newColTuple);
  }

  // if it doesn’t exist, create a BTree index for the given column
  public boolean createBTreeIndex(int column)
      throws GetFileEntryException, PinPageException, ConstructPageException,
      HFException, InvalidTupleSizeException, HFBufMgrException,
      HFDiskMgrException, IOException, KeyTooLongException,
      KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
      UnpinPageException, NodeNotMatchException, ConvertException,
      DeleteRecException, IndexSearchException, IteratorException,
      LeafDeleteException, InsertException, CFException, AddFileEntryException, PageUnpinnedException,
      InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
    BTreeFile btf;
    String btFileName = new String(_fileName + ".btree" + Integer.toString(column));
    AttrType keyTypeArg = type[column - 1];
    int deleteType = 1; // DeleteFashion.FULL_DELETE argument
    int maxKeySizeArg;
    if (AttrType.attrInteger == keyTypeArg.attrType) {
      maxKeySizeArg = 5;
    } else if (AttrType.attrString == keyTypeArg.attrType) {
      maxKeySizeArg = GlobalConst.MAX_NAME;
    } else {
      System.out.println("Invalid attribute type only handled for Integer and String types.");
      throw new CFException(null, "Invalid attribute type only handled for Integer and String types.");
    }
    // Create the BTree
    btf = new BTreeFile(btFileName, keyTypeArg.attrType, maxKeySizeArg, deleteType);
    RID rid = new RID();
    Scan scan = this.openColumnScan(column);
    try {
      Tuple tuple = scan.getNext(rid);
      while (tuple != null) {
        if (type[column - 1].attrType == AttrType.attrInteger) {
          int intValue = Convert.getIntValue(0, tuple.getTupleByteArray());
          btf.insert(new IntegerKey(intValue), rid);
        }
        if (type[column - 1].attrType == AttrType.attrString) {
          byte[] byteArr = tuple.getTupleByteArray();
          String stringValue = Convert.getStrValue(0, byteArr, byteArr.length);
          btf.insert(new StringKey(stringValue), rid);
        }
        tuple = scan.getNext(rid);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      scan.closescan();
      btf.close();
    }
    return true;
  }

  // if it doesn’t exist, create a bitmap index for the given column and value
  public boolean createBitMapIndex(int columnNo, ValueClass value)
      throws GetFileEntryException, ConstructPageException,
      IOException, AddFileEntryException, HFBufMgrException,
      HFException, HFDiskMgrException {
	try
	{
		// note -- the value in the ValueClass is arbitrary for this constructor
		// the main focus is getting a column type argument passed in correctly
		String bmFileName;
		bmFileName = new String(_fileName + ".bitmap" + Integer.toString(columnNo));
		BitMapFile tmpBMF = new BitMapFile(bmFileName, this, columnNo, value);
		// we do not store the BitMap object return because the constructor creates
		// a file that we can later access by the same name
		//tmpBMF.close();
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}

    return true;
  }

  // add the tuple to a heapfile tracking the deleted tuples from the columnar
  // file
  public boolean markTupleDeleted(TID tid)
      throws IOException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      InvalidSlotNumberException,
      InvalidTupleSizeException,
      SpaceNotAvailableException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException {
    byte[] data = new byte[8 + numColumns * 8];
    tid.writeToByteArray(data, 0);
    new Heapfile(_fileName + ".deleted").insertRecord(data);
    return true;
  }

  // merge all deleted tuples from the file as well as all from all index files.
  public boolean purgeAllDeletedTuples()
      throws IOException,
      HFDiskMgrException,
      HFBufMgrException,
      HFException,
      InvalidTupleSizeException,
      InvalidSlotNumberException,
      CFException,
      Exception {
    Heapfile deletedHf = new Heapfile(_fileName + ".deleted");
    Scan scan = deletedHf.openScan();
    Tuple tuple = scan.getNext(new RID());
    while (tuple != null) {
      TID tid = new TID(tuple.getTupleByteArray(), 0);
      boolean isDeleted = this._deleteTuple(tid);
      if (isDeleted == false) {
        throw new CFException(null, "purgeAllDeletedTuples failed");
      }
      tuple = scan.getNext(new RID());
    }
    scan.closescan();
    TupleScan tupleScan = this.openTupleScan();
    TID tid = new TID(Columnarfile.numColumns, -1, new RID[numColumns]);
    Tuple toBeReinserted = new Tuple();
    TID toBeReinsertedTID = new TID(Columnarfile.numColumns);
    tuple = tupleScan.getNext(tid);
    while (tuple != null) {
      toBeReinserted = new Tuple(tuple);
      toBeReinsertedTID.copyTid(tid);
      tuple = tupleScan.getNext(tid);
      this._deleteTuple(toBeReinsertedTID);
      this.insertTuple(toBeReinserted.getTupleByteArray());
    }
    deletedHf.deleteFile();
    deletedHf = new Heapfile(_fileName + ".deleted");
    return true;
  }

  // return the position of the rid in the columnar files otherwise -1
  public int getPositionFromRid(RID rid, int column)
      throws InvalidTupleSizeException,
      IOException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException {
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(column));
    try {
      return hf.getPositionFromRid(rid);
    } catch (Exception e) {
      throw new HFException(e, "getPositionFromRid failed");
    }
  }

  // return the rid from the position in the columnar files otherwise throws
  // exception
  public RID getRidFromPosition(int position, int column)
      throws InvalidTupleSizeException,
      IOException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      CFException {
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(column));
    try {
      return hf.getRidFromPosition(position);
    } catch (Exception e) {
      throw new CFException(e, "getRidFromPosition failed");
    }
  }

  // return the tid from the position in the columnar files otherwise throws
  // exception
  public TID getTidFromPosition(int position)
      throws InvalidTupleSizeException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      CFException,
      IOException {
    RID[] recordIDs = new RID[numColumns];
    for (int i = 0; i < numColumns; i++) {
      recordIDs[i] = getRidFromPosition(position, i + 1);
    }
    return new TID(numColumns, position, recordIDs);
  }

  private boolean _deleteTuple(TID tid)
      throws InvalidSlotNumberException,
      InvalidTupleSizeException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      Exception {
    boolean isDeleted = true;
    for (int i = 0; i < numColumns; i++) {
      Heapfile colHf = new Heapfile(_fileName + "." + Integer.toString(i + 1));
      boolean isColDeleted = colHf.deleteRecord(tid.recordIDs[i]);
      if (isColDeleted == false)
        isDeleted = false;
    }
    return isDeleted;
  }

  public String getFileName() {
    return _fileName;
  }

}
