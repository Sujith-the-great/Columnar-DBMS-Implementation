/*
 * File - BitMapFile.java
 *
 * Description - 
 *		...
 */
package bitmap;

import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;
import btree.*;
import columnar.*;


/*  
 * CBitMapFile Class
 */
public class CBitMapFile extends IndexFile
	implements GlobalConst
{
	//----------------
	//Class Variables
	//----------------
	private Columnarfile srcColumnar;
	private CBitMapHeaderPage headerPage;
	private PageId  headerPageId;
	private String  dbname;
	private int columnMap = -1;
	private int mapType = AttrType.attrNull;
	
	//----------------
	//Helpful constatns
	//----------------
	private static final int SIZEOF_BYTE = 1;
	private static final int SIZEOF_SHORT = 2;
	private static final int SIZEOF_INT = 4;
	
	//----------------
	//Constructors
	//----------------
	
	//CBitMapFile class; an index file with given filename
	//should already exist, then this opens it.
	//mirror from BTreeFile
	public CBitMapFile( String filename )
		throws GetFileEntryException,  
		   PinPageException, 
		   ConstructPageException    
	{
		headerPageId = get_file_entry(filename);
		headerPage = new CBitMapHeaderPage( headerPageId );
		dbname = new String(filename);
	}
	
	//CBitMapFile class
	//Takes a columnar file, a column to map, and the ValueClass type to map
	//Checks if the passed filename exists to open, else makes a new one
	//if a new one is made it will iterate each record in the corresponding
	//column's heapfile and insert it into the bitmap
	public CBitMapFile( String filename, Columnarfile columnfile,
						int ColumnNo, ValueClass value )
		throws GetFileEntryException, ConstructPageException,
				IOException, AddFileEntryException, HFBufMgrException,
				HFException, HFDiskMgrException
	{
		//get the id of the page for the passed filename
		headerPageId = get_file_entry(filename);
		//file not exist, create one
		if( headerPageId == null )
		{
			//assocaite the columnarfile input
			srcColumnar = columnfile;
			//store the column count
			columnMap = ColumnNo;
			//get the type of map this is (string/int)
			mapType = value.getType();
			
			//define a Bitmap Header page
			headerPage = new CBitMapHeaderPage(); 
			headerPage.init( value );
			headerPageId = headerPage.headBMPageId;
			
			add_file_entry(filename, headerPageId);
			
			//Map column values from columnar file into the bitmap
			/*
			---Pseudo-code of bitmap creation---
			for page in HeapFile@columnNo
				for record in page
					Get record-value
					Get record-page-#
					Get record-slot-#
					headerPage.insertMap( record-value, record-page-#, record-slot-# )
			*/
			
			//iterate all pages
			RID rid = new RID();
			try
			{
				//open a scan of the corresponding columnar column Heapfile
				Scan scan = columnfile.openColumnScan(columnMap);
				Tuple tuple = scan.getNext(rid);
				//iterate and insert each record
				while (tuple != null)
				{
					if (columnfile.type[columnMap - 1].attrType == AttrType.attrInteger)
					{
						int intValue = Convert.getIntValue(0, tuple.getTupleByteArray());
						IntegerValueClass tmpValueClass = new IntegerValueClass( intValue );
						//insert the value, and a pointer back to the space in the Heapfile mapped
						headerPage.insertMap(tmpValueClass, (short)rid.pageNo.pid, (short)rid.slotNo);
					}
					if (columnfile.type[columnMap - 1].attrType == AttrType.attrString)
					{
						byte[] byteArr = tuple.getTupleByteArray();
						String stringValue = Convert.getStrValue(0, byteArr, byteArr.length);
						StringValueClass tmpValueClass = new StringValueClass( stringValue );
						//insert the value, and a pointer back to the space in the Heapfile mapped
						headerPage.insertMap(tmpValueClass, (short)rid.pageNo.pid, (short)rid.slotNo);
					}
					//get next entry in scan
					tuple = scan.getNext(rid);
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		else //else opening existing file
		{
			System.out.println("\tDEBUG -- open existing");
			headerPage = new CBitMapHeaderPage( headerPageId );
			unpinPage(headerPageId, true);
		}
		
		dbname = new String(filename);
	}
	
	//----------------
	//Page Manipulation Methods
	//(mirrored from BTreeFile.java)
	//----------------
	
	private PageId get_file_entry(String filename)
		throws GetFileEntryException
	{
		try
		{
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new GetFileEntryException(e,"");
		}
	}
	
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
	
	private Page pinPage(PageId pageno)
		throws PinPageException
	{
		try
		{
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
			return page;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new PinPageException(e,"");
		}
	}
  
	private void add_file_entry(String fileName, PageId pageno)
		throws AddFileEntryException
	{
		try
		{
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new AddFileEntryException(e,"");
		}
	}
  
	private void unpinPage(PageId pageno)
		throws UnpinPageException
	{ 
		try
		{
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		}
	}
	  
	private void freePage(PageId pageno)
		throws FreePageException
	{
		try
		{
			SystemDefs.JavabaseBM.freePage(pageno);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new FreePageException(e,"");
		}
	}
	
	private PageId newPage(Page page, int num)
		throws HFBufMgrException
	{
		PageId tmpId = new PageId();
		try
		{
			tmpId = SystemDefs.JavabaseBM.newPage(page,num);
		}
		catch (Exception e)
		{
			throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
		}

		return tmpId;
	}
		
	private void delete_file_entry(String filename)
		throws DeleteFileEntryException
	{
		try
		{
			SystemDefs.JavabaseDB.delete_file_entry( filename );
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new DeleteFileEntryException(e,"");
		}
	}
	
	//----------------
	//Accessor Methods
	//----------------
	
	//Access method to member data
	public CBitMapHeaderPage getHeaderPage()
	{
		return headerPage;
	}
	
	//----------------
	//Functional Methods
	//----------------
	
	//Close the CBitMap File
	//mirror from BTreeFile::close()
	public void close()
		throws PageUnpinnedException, InvalidFrameNumberException,
				HashEntryNotFoundException, ReplacerException
	{
		//check a valid file is being closed
		if ( headerPage != null )
		{
			//unpin the page and set its reference to null
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}
	
	//Destroy entire BitMap file
	//mirror from BTreeFile::destroyBTreeFile()
	public void destroyBitMapFile()
		throws IOException, IteratorException, UnpinPageException,
				FreePageException, DeleteFileEntryException,
				ConstructPageException, PinPageException
	{
		//confirm non-null page to destroy
		if( headerPage != null)
		{
			unpinPage(headerPageId);
			freePage(headerPageId);      
			delete_file_entry(dbname);
			headerPage = null;
		}
	}
	
	/*
	 * Takes a position argument which corresponds to an unique entry in
	 * the columnar file. That position also correlates to an entry in the
	 * column's Heapfile.
	 * This method iterates the heapfile and finds the position in a given HFPage
	 * and the slot #. This method then adds that value to the bitmaps in this
	 * file and associates the page # & slot # to the HFPage entry.
	 * If no entry exists in the column Heapfile for the passed position, then
	 * this method will exit without changing this bitmap file
	 */
	public boolean Insert( int position )
		throws CFException, HFException, HFBufMgrException,
				HFDiskMgrException, IOException,
				InvalidTupleSizeException
	{
		boolean successfulInsert = false;
		
		//Get the RID (page # & Slot #) of data being mapped
		RID positionRID = srcColumnar.getRidFromPosition( position, columnMap );
		PageId pageMatch = positionRID.pageNo;
		int slotEntryInPage = positionRID.slotNo;
		
		HFPage curHFPage = new HFPage();
		
		//pin the page with data we intend to insert
		pinPage(pageMatch, curHFPage, false/*read disk*/);
		
		//get data needed to create bitmap entry
		short lengthOfData = curHFPage.getSlotLength( slotEntryInPage );
		short offsetOfData = curHFPage.getSlotOffset( slotEntryInPage );
		
		//insert with appropriate mapping type to the data
		if( AttrType.attrString == mapType )
		{
			String tmpString = Convert.getStrValue( offsetOfData, curHFPage.getHFpageArray(), lengthOfData );
			StringValueClass tmpValueClass = new StringValueClass( tmpString );
			successfulInsert = headerPage.insertMap( tmpValueClass, (short)pageMatch.pid, (short)slotEntryInPage );
		}
		else //default to int
		{
			int tmpInt = Convert.getIntValue( offsetOfData, curHFPage.getHFpageArray() );
			IntegerValueClass tmpValueClass = new IntegerValueClass( tmpInt );
			successfulInsert = headerPage.insertMap( tmpValueClass, (short)pageMatch.pid, (short)slotEntryInPage );
		}
		
		//unpin the page at completion of operation
		unpinPage(pageMatch, false/*not DIRTY*/);
		
		//else the position requested is in none of the heap pages and the data
		//cannot be properly inserted
		
		return successfulInsert;
	}
	
	/*
	 * Takes a position argument which corresponds to an unique entry in
	 * the columnar file. That position also correlates to an entry in the
	 * column's Heapfile.
	 * This method iterates the heapfile and finds the position in a given HFPage
	 * and the slot #. This method then finds the matching page # & slot # entry
	 * in the bitmap file and deletes the mapping
	 * If no entry exists in the column Heapfile for the passed position, then
	 * this method will exit without changing this bitmap file
	 */
	public boolean Delete( int position )
		throws CFException, HFBufMgrException, HFException, HFDiskMgrException,
				IOException, InvalidSlotNumberException,
				InvalidTupleSizeException
	{
		boolean successfulDelete = false;
		
		//Get the RID (page # & Slot #) of data being mapped
		RID positionRID = srcColumnar.getRidFromPosition( position, columnMap );
		PageId pageMatch = positionRID.pageNo;
		int slotEntryInPage = positionRID.slotNo;
		
		//using the page # & slot #, find and delete the coresponding bitmap entry
		successfulDelete = headerPage.deleteMap( (short)pageMatch.pid, (short)slotEntryInPage );
		
		return successfulDelete;
	}
	
	/** create a scan with given keys
	 * Cases:
	 *      (1) lo_key = null, hi_key = null
	 *              scan the whole index
	 *      (2) lo_key = null, hi_key!= null
	 *              range scan from min to the hi_key
	 *      (3) lo_key!= null, hi_key = null
	 *              range scan from the lo_key to max
	 *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
	 *              exact match ( might not unique)
	 *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	 *              range scan from lo_key to hi_key
	 *@param lo_key the key where we begin scanning. Input parameter.
	 *@param hi_key the key where we stop scanning. Input parameter.
	 *@exception IOException error from the lower layer
	 *@exception KeyNotMatchException key is not integer key nor string key
	 *@exception IteratorException iterator error
	 *@exception ConstructPageException error in BT page constructor
	 *@exception PinPageException error when pin a page
	 *@exception UnpinPageException error when unpin a page
	 */
	public CBMFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
		throws IOException, KeyNotMatchException, IteratorException, 
			ConstructPageException, PinPageException, UnpinPageException
	{
		CBMFileScan scan = new CBMFileScan();
		//return an empty scan if there is no header page for this file
		if ( INVALID_PAGE == headerPage.headBMPageId.pid )
		{
			scan.curPage = null;
			return scan;
		}
		
		try
		{
			CBMPage head = new CBMPage();
			pinPage( headerPage.headBMPageId, head, false );
			
			scan.keyType = head.getMapType();
			//convert the low/high key values to comparable bitmaps
			if( AttrType.attrString == head.getMapType() )
			{
				scan.lowKey = head.convertStringToMap( ((StringKey)lo_key).getKey() );
				scan.highKey = head.convertStringToMap( ((StringKey)hi_key).getKey() );
			}
			else
			{
				scan.lowKey = head.convertIntToMap( ((IntegerKey)lo_key).getKey() );
				scan.highKey = head.convertIntToMap( ((IntegerKey)hi_key).getKey() );
			}
			scan.curRid = null;
			scan.bmFilename = dbname;
			scan.didfirst = false;
			scan.deletedcurrent = false;
			scan.curSlot = 0;
			scan.bmfile = this;
			
			//Sets up scan at the starting position, ready for iteration
			scan.curPage = head;
			scan.curPageId = head.getCurPage();
			
			unpinPage(headerPage.headBMPageId, false/*not DIRTY*/);
			
		} catch (Exception e){}
		
		return scan;
	}
	
	//----------------------
	//Simple implementation of IndexFile abstract methods
	//names are self explanatory to function
	//----------------------
	
	public void insert(final KeyClass data, final RID rid)
		throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,   
			IndexInsertRecException,ConstructPageException, UnpinPageException,
			PinPageException, NodeNotMatchException, ConvertException,
			DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException
	{
		//Due to some "throws" complications, ajva does not allow us to use the logic inside
		//thise try catch without throwing appropriate exceptions. However, we cannot "throw"
		//them as part of the method signature since this method defines the abstract method
		//IndexFile.Delete(), so the solution is to put all exception throwin logic in a try/catch
		try
		{
			//we take in the KeyClass object to satisfy the abstract method signature, but
			//in this implementation it is not used
			PageId pageMatch = rid.pageNo;
			int slotEntryInPage = rid.slotNo;
			
			HFPage curHFPage = new HFPage();
			
			//pin the page with data we intend to insert
			pinPage(pageMatch, curHFPage, false/*read disk*/);
			
			//get data needed to create bitmap entry
			short lengthOfData = curHFPage.getSlotLength( slotEntryInPage );
			short offsetOfData = curHFPage.getSlotOffset( slotEntryInPage );
			
			//insert with appropriate mapping type to the data
			if( AttrType.attrString == mapType )
			{
				String tmpString = Convert.getStrValue( offsetOfData, curHFPage.getHFpageArray(), lengthOfData );
				StringValueClass tmpValueClass = new StringValueClass( tmpString );
				headerPage.insertMap( tmpValueClass, (short)pageMatch.pid, (short)slotEntryInPage );
			}
			else //default to int
			{
				int tmpInt = Convert.getIntValue( offsetOfData, curHFPage.getHFpageArray() );
				IntegerValueClass tmpValueClass = new IntegerValueClass( tmpInt );
				headerPage.insertMap( tmpValueClass, (short)pageMatch.pid, (short)slotEntryInPage );
			}
			
			//unpin the page at completion of operation
			unpinPage(pageMatch, false/*not DIRTY*/);
		
		}catch ( Exception e ) {}
	}
	
	public boolean Delete(final KeyClass data, final RID rid)  
		throws  DeleteFashionException, LeafRedistributeException,RedistributeException,
			InsertRecException, KeyNotMatchException, UnpinPageException,
			IndexInsertRecException, FreePageException, RecordNotFoundException, 
			PinPageException, IndexFullDeleteException, LeafDeleteException,
			IteratorException, ConstructPageException, DeleteRecException,
			IndexSearchException, IOException
	{
		boolean succesfulDelete = false;
		
		//Due to some "throws" complications, ajva does not allow us to use the logic inside
		//thise try catch without throwing appropriate exceptions. However, we cannot "throw"
		//them as part of the method signature since this method defines the abstract method
		//IndexFile.Delete(), so the solution is to put all exception throwin logic in a try/catch
		try
		{
			//we take in the KeyClass object to satisfy the abstract method signature, but
			//in this implementation it is not used
			PageId pageMatch = rid.pageNo;
			int slotEntryInPage = rid.slotNo;
			
			//using the page # & slot #, find and delete the coresponding bitmap entry
			succesfulDelete = headerPage.deleteMap( (short)pageMatch.pid, (short)slotEntryInPage );
		
		}catch ( Exception e ) {}
		
		return succesfulDelete;
	}
}