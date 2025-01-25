package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;

import bitmap.BMFileScan;
import bitmap.BitMapFile;

/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. It will also perform selections and projections.
 * information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class ColumnIndexScan extends Iterator {

  /**
   * class constructor. set up the index scan.
   * 
   * @param index     type of the index (B_Index, Hash)
   * @param relName   name of the input relation
   * @param indName   name of the input index
   * @param type      array of types in this relation
   * @param str_sizes array of string sizes (for attributes that are string)
   * @param selects   conditions to apply, first one is primary
   * @param indexOnly whether the answer requires only the key or the tuple
   * @exception IndexException            error from the lower layer
   * @exception InvalidTypeException      tuple type not valid
   * @exception InvalidTupleSizeException tuple size not valid
   * @exception UnknownIndexTypeException index type unknown
   * @exception IOException               from the lower layer
   */
  public ColumnIndexScan(
      IndexType index,
      final String relName,
      final String indName,
      AttrType type,
      short str_sizes,
      CondExpr selects[],
      final boolean indexOnly)
      throws IndexException,
      InvalidTypeException,
      InvalidTupleSizeException,
      UnknownIndexTypeException,
      IOException {
    _type = type;
    _s_sizes = str_sizes;
    _selects = selects;
    index_only = indexOnly;

    try {
      f = new Heapfile(relName);
    } catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: Heapfile not created");
    }

    switch (index.indexType) {
      // linear hashing is not yet implemented
      case IndexType.B_Index:
        // error check the select condition
        // must be of the type: value op symbol || symbol op value
        // but not symbol op symbol || value op value
        try {
          indFile = new BTreeFile(indName);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
        }

        try {
          indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
        }
        break;

      case IndexType.Bitmap:
        try {
          indFile = new BitMapFile(indName);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: BM exceptions caught from BitMapFile constructor");
        }

        try {
          indScan = (BMFileScan) IndexUtils.BM_scan(selects, indFile);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: BM exceptions caught from IndexUtils.BM_scan().");
        }
        break;

      case IndexType.CBitmap:
        try {
          indFile = new CBitMapFile(indName);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: CBM exceptions caught from CBitMapFile constructor");
        }

        try {
          indScan = (CBMFileScan) IndexUtils.BM_scan(selects, indFile);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: CBM exceptions caught from IndexUtils.CBM_scan().");
        }
        break;
      case IndexType.None:
      default:
        throw new UnknownIndexTypeException("Only BTree and Bitmap index is supported so far");

    }

  }

  /**
   * returns the next tuple.
   * if <code>index_only</code>, only returns the key value
   * (as the first field in a tuple)
   * otherwise, retrive the tuple and returns the whole tuple
   * 
   * @return the tuple
   * @exception IndexException          error from the lower layer
   * @exception UnknownKeyTypeException key type unknown
   * @exception IOException             from the lower layer
   */
  public KeyDataEntry get_next_KeyDataEntry()
      throws IndexException,
      UnknownKeyTypeException,
      IOException {
    RID rid;
    int unused;
    KeyDataEntry nextentry = null;

    try {
      nextentry = indScan.get_next();
    } catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: BTree error");
    }

    while (nextentry != null) {
      // only need to return the key

      AttrType[] attrType = new AttrType[1];
      short[] s_sizes = new short[1];
      Jtuple = new Tuple();

      if (_type.attrType == AttrType.attrInteger) {
        attrType[0] = new AttrType(AttrType.attrInteger);
        try {
          Jtuple.setHdr((short) 1, attrType, s_sizes);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        try {
          Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey().intValue());
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
      } else if (_type.attrType == AttrType.attrString) {

        attrType[0] = new AttrType(AttrType.attrString);
        try {
          Jtuple.setHdr((short) 1, attrType, s_sizes);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        try {
          Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
      } else {
        // attrReal not supported for now
        throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
      }
      rid = ((LeafData) nextentry.data).getData();
      return nextentry;
    }
    return null;
  }

  public Tuple get_next()
      throws IndexException,
      UnknownKeyTypeException,
      IOException {
    RID rid;
    int unused;
    KeyDataEntry nextentry = null;

    try {
      nextentry = indScan.get_next();
    } catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: BTree error");
    }

    while (nextentry != null) {
      // only need to return the key

      AttrType[] attrType = new AttrType[1];
      short[] s_sizes = new short[1];

      if (_type.attrType == AttrType.attrInteger) {
        attrType[0] = new AttrType(AttrType.attrInteger);
        try {
          Jtuple.setHdr((short) 1, attrType, s_sizes);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        try {
          Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey().intValue());
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
      } else if (_type.attrType == AttrType.attrString) {

        attrType[0] = new AttrType(AttrType.attrString);
        try {
          Jtuple.setHdr((short) 1, attrType, s_sizes);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        try {
          Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
      } else {
        // attrReal not supported for now
        throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
      }
      rid = ((LeafData) nextentry.data).getData();
      return Jtuple;
    }
    return null;
  }

  /**
   * Cleaning up the index scan, does not remove either the original
   * relation or the index from the database.
   * 
   * @exception IndexException error from the lower layer
   * @exception IOException    from the lower layer
   */
  public void close() throws IOException, IndexException {
    if (!closeFlag) {
      if (indScan instanceof BTFileScan) {
        try {
          ((BTFileScan) indScan).DestroyBTreeFileScan();
          ((BTreeFile)indFile).close();
        } catch (Exception e) {
          throw new IndexException(e, "BTree error in destroying index scan.");
        }
      }
      if (indScan instanceof BMFileScan) {
        try {
          ((BitMapFile)indFile).close();
        } catch (Exception e) {
          throw new IndexException(e, "BM error in destroying index scan.");
        }
      }

      closeFlag = true;
    }
  }

  private IndexFile indFile;
  private IndexFileScan indScan;
  private AttrType _type;
  private short _s_sizes;
  private CondExpr[] _selects;
  private Heapfile f;
  private boolean index_only;
  private Tuple tuple1;
  private Tuple Jtuple;

}
