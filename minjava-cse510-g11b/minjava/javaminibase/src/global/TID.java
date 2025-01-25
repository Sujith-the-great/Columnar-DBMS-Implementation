/*
 * File - TID.java
 *
 * Original Author - Vivian Roshan Adithan
 *
 * Description - 
 *		...
 */
package global;

import java.io.IOException;

public class TID {
  public int numRIDs;
  public int position;
  public RID[] recordIDs;

  public TID(int numRIDs) {
    this.numRIDs = numRIDs;
    this.position = -1;
    this.recordIDs = new RID[numRIDs];
    for (int i = 0; i < numRIDs; i++) {
      this.recordIDs[i] = new RID();
    }
  }

  public TID(int numRIDs, int position) {
    this.numRIDs = numRIDs;
    this.position = position;
    this.recordIDs = new RID[numRIDs];
    for (int i = 0; i < numRIDs; i++) {
      this.recordIDs[i] = new RID();
    }
  }

  public TID(int numRIDs, int position, RID[] recordIDs) {
    this.numRIDs = numRIDs;
    this.position = position;
    this.recordIDs = new RID[this.numRIDs];
    for (int i = 0; i < numRIDs; i++) {
      this.recordIDs[i] = new RID();
      if (recordIDs[i] != null)
        this.recordIDs[i].copyRid(recordIDs[i]);
    }
  }

  public TID(byte[] array, int offset)
      throws IOException {
    this.numRIDs = Convert.getIntValue(offset, array);
    this.position = Convert.getIntValue(offset + 4, array);
    this.recordIDs = new RID[this.numRIDs];
    for (int i = 0; i < this.numRIDs; i++) {
      this.recordIDs[i] = constructRID(array, offset + 8 + i * 8);
    }
  }

  public RID constructRID(byte[] array, int offset)
      throws IOException {
    int slotNo = Convert.getIntValue(offset, array);
    int pageNo = Convert.getIntValue(offset + 4, array);
    PageId pageId = new PageId(pageNo);
    return new RID(pageId, slotNo);
  }

  /**
   * make a copy of the given tid
   */
  public void copyTid(TID tid) {
    this.numRIDs = tid.numRIDs;
    this.position = tid.position;
    this.recordIDs = new RID[this.numRIDs];
    for (int i = 0; i < this.numRIDs; i++) {
      this.recordIDs[i] = new RID();
      if (tid.recordIDs[i] != null)
        this.recordIDs[i].copyRid(tid.recordIDs[i]);
    }
  }

  /**
   * Compares two TID object, i.e, this to the tid
   * 
   * @param tid TID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  public boolean equals(TID tid) {
    return this.numRIDs == tid.numRIDs
        && this.position == tid.position
        && this.recordIDs == tid.recordIDs;
  }

  /**
   * Write the tid into a byte array at offset
   * 
   * @param array
   * @param offset
   * @throws IOException
   */
  public void writeToByteArray(byte[] array, int offset)
      throws IOException {
    Convert.setIntValue(this.numRIDs, offset, array);
    Convert.setIntValue(this.position, offset + 4, array);
    for (int i = 0; i < this.numRIDs; i++) {
      this.recordIDs[i].writeToByteArray(array, offset + 8 + i * 8);
    }
  }

  public void setPosition(int position) {
    this.position = position;
  }

  public void setRID(int column, RID recordID) {
    this.recordIDs[column] = recordID;
  }
}
