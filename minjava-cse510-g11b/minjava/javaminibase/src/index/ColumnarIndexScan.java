package index;

import java.io.IOException;

import btree.KeyDataEntry;
import btree.LeafData;
import columnar.Columnarfile;
import global.*;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.*;

public class ColumnarIndexScan extends Iterator {

  private ColumnIndexScan _columnIndexScan;
  private boolean _indexOnly;
  private String _relName;
  private int fldNum;
  private int noInFlds;
  private int noOutFlds;
  FldSpec[] outFlds;
  AttrType[] types;
  short[] strSizes;

  public ColumnarIndexScan(
      java.lang.String relName,
      int[] fldNum,
      IndexType index,
      String[] indName,
      AttrType[] types,
      short[] str_sizes,
      int noInFlds,
      int noOutFlds,
      FldSpec[] outFlds,
      CondExpr[] selects,
      boolean indexOnly)
      throws IndexException,
      InvalidTypeException,
      InvalidTupleSizeException,
      UnknownIndexTypeException,
      IOException {
    this._indexOnly = indexOnly;
    this.fldNum = fldNum[0];
    this.noInFlds = noInFlds;
    this.noOutFlds = noOutFlds;
    this.outFlds = outFlds;
    this.types = types;
    this.strSizes = str_sizes;
    this._relName = relName;
    int count = 0;
    short str_size = 0;
    if (types[this.fldNum - 1].attrType == AttrType.attrString) {
      for (int i = 0; i < this.fldNum; i++) {
        if (types[i].attrType == AttrType.attrString) {
          count++;
        }
      }
      str_size = str_sizes[count];
    }
    _columnIndexScan = new ColumnIndexScan(
        index, relName + "." + Integer.toString(this.fldNum),
        indName[this.fldNum - 1], types[this.fldNum - 1], str_size, selects,
        indexOnly);
  }

  public Tuple get_next()
      throws Exception {
    if (_indexOnly) {
      return _columnIndexScan.get_next();
    } else {
      KeyDataEntry entry = _columnIndexScan.get_next_KeyDataEntry();
      if (entry == null) {
        return null;
      }
      RID rid = ((LeafData) entry.data).getData();
      Columnarfile f = new Columnarfile(_relName);
      int position = f.getPositionFromRid(rid, this.fldNum);
      TID tid = f.getTidFromPosition(position);
      Tuple tuple = f.getTuple(tid);
      tuple.setHdr((short) noInFlds, types, strSizes);
      Tuple Jtuple = new Tuple();
      AttrType[] Jtypes = new AttrType[noOutFlds];
      short[] ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, f.type,
          f.type.length, f.strSizes, outFlds, noOutFlds);
      Projection.Project(tuple, f.type, Jtuple, outFlds, noOutFlds);
      return Jtuple;
    }
  }

  public TID get_next_TID()
      throws Exception {
    KeyDataEntry entry = _columnIndexScan.get_next_KeyDataEntry();
    if (entry == null) {
      return null;
    }
    RID rid = ((LeafData) entry.data).getData();
    Columnarfile f = new Columnarfile(_relName);
    int position = f.getPositionFromRid(rid, this.fldNum);
    return f.getTidFromPosition(position);
  }

  public void close() throws IOException, IndexException {
    if (!closeFlag) {
      _columnIndexScan.close();
    }
  }

}
