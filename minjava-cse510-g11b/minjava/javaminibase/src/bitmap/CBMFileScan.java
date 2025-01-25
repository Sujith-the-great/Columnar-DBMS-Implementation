/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package bitmap;
import java.io.*;
import global.*;
import heap.*;
import btree.*;
import diskmgr.*;
import bufmgr.*;

/**
 * BMFileScan implements a search/iterate interface to B+ tree 
 * index files (class BMFileScan).  It derives from abstract base
 * class IndexFileScan.  
 */
public class CBMFileScan  extends IndexFileScan
             implements  GlobalConst
{
	CBitMapFile bmfile; 
	String bmFilename;     // Bitmap we're scanning
	CBMPage curPage;			//current BMPage being scanned
	PageId curPageId;		//current PageId being scanned
	RID curRid;
	int curSlot;    		// position in current mapping; note: this is 
							// the RID of the key/RID pair within the BMpage.                                    
	boolean didfirst;       // false only before getNext is called
	boolean deletedcurrent; // true after deleteCurrent is called (read
							// by get_next, written by deleteCurrent).
	
	byte[] lowKey;		//The min value to return in this scan
	byte[] highKey;		//The max value to return in this scan
	
	int keyType;
	int maxKeysize;
	
	//-----------------
	//Buffer support methods
	//-----------------
	private void pinPage(PageId pageno, Page page, boolean emptyPage)
		throws HFBufMgrException
	{
		try
		{
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		}
		catch (Exception e)
		{
			throw new HFBufMgrException(e,"BitmapHeaderFile.java: pinPage() failed");
		}
	}
	
	private void unpinPage(PageId pageno, boolean dirty)
		throws HFBufMgrException
	{
		try
		{
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		}
		catch (Exception e)
		{
			throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
		}
	}
	
	
	/**
	 * Get the next record within [minKey, maxKey] bounds
	 * @exception ScanIteratorException error when iterating through the records
	 * @return the KeyDataEntry, which contains the key and data
	 * (based on BTreeFileScan logic of same method)
	 */
	public KeyDataEntry get_next()
		throws ScanIteratorException
	{
		KeyDataEntry entry = null;
		PageId nextpage;
		
		try
		{
			if( curPage != null )
			{
				boolean inRange = false;
				while( inRange )
				{
					//pin page to read
					pinPage( curPageId, curPage, false );
					
					//get the number of maps on this page
					int mapCount = (int)curPage.getMapCnt();
					//iterate from last slot to check to end of page
					for(int map = curSlot; map < mapCount; map++)
					{
						//assume true until proven out of range
						boolean nextMatch = true;
						byte[] nextMap = curPage.getMapAtSlot( map );
						//check each compnent of the map is in range
						for(int i = 0; i < nextMap.length; i++)
						{
							//because string maps are variable length, we need to
							//check comparison maps are still in range
							if( i < lowKey.length )
							{
								//check if this component is less than the min bound
								if( nextMap[i] < lowKey[i] )
									nextMatch = false;
							}
							if( i < lowKey.length )
							{
								//check if this component is less than the min bound
								if( nextMap[i] > highKey[i] )
									nextMatch = false;
							}
						}
						//check if the checked map is in search range of scan
						if( nextMatch )
						{
							inRange = true;
							//set the current RID of the match
							curRid = new RID( new PageId(curPage.getPtrPage(map)), curPage.getPtrSlot(map) );
							//get the mapped value and store it in the returned KeyEntry
							if( (int)AttrType.attrString == keyType )
							{
								String tmpString = curPage.convertMapToString( nextMap );
								entry = new KeyDataEntry( tmpString, curRid );
							}
							else
							{
								int tmpInt = curPage.convertMapToInt( nextMap );
								entry = new KeyDataEntry( tmpInt, curRid );
							}
							//set the "curSlot" to the next slot for subsequent
							//call of get_next()
							curSlot = map + 1;
							//quick exit map loop
							map = mapCount;
						}
					}
					//if inRange is still false by here, we'll need to get the next page
					if( false == inRange )
					{
						PageId nextPageId = curPage.getNextPage();
						//if we reach the end of the list, exit
						if( -1 == nextPageId.pid )
						{
							inRange = true;
						}
						unpinPage(curPageId, false /*not DIRTY*/);
						curPageId = nextPageId;
						//reset curSlot to 0 for new page
						curSlot = 0;
					}
					else
					{
						//else unpin page and be prepared to return the value
						unpinPage(curPageId, false /*not DIRTY*/);
					}
				}
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			throw new ScanIteratorException();
		}
		
		return entry;
	}
	
	/** 
	 * Delete the current record.
	 * @exception ScanDeleteException delete current record failed
	 * (based on BTreeFileScan logic of same method)
	 */
	public void delete_current()
		throws ScanDeleteException
	{
		try
		{  
			if (curPage == null) {
				System.out.println("No Record to delete!"); 
				throw new ScanDeleteException();
			}
			
			//if there is a pointed to RID to delete
			if( null != curRid )
			{
				//delete the entry by RID (KeyValue arg won't be used)
				deletedcurrent = bmfile.Delete( (KeyClass)null, curRid );
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new ScanDeleteException();
		}  
	}
	
	/**
	 * Returns the size of the key
	 * @return the keysize
	 * (based on BTreeFileScan logic of same method)
	 */
	public int keysize()
	{
		return maxKeysize;
	}
	
}
