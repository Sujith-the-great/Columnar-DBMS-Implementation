/*
 * File - CBitMapHeaderPage.java
 *
 * Description - 
 *		Header page for the Bitmap file that
 *		performs management functions over all
 *		pages in the file
 */
package bitmap;

import java.io.*;
import java.lang.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import btree.*;

/*  
 * CBitMapHeaderPage Class
 *
 * Has a pointer the first page in the list of BMPages
 * for a particular bitmap file.
 * It is expected for each BitMapFile class object to have
 * one BitMapHeaderPage, and each Header Page has a single
 * Page ID for the start of the page list. The Pages can be
 * traversed in a doubly linked list manner so long as this
 * head value is maintained.
 */
public class CBitMapHeaderPage extends HFPage
{
	//----------------
	//Class Variables
	//----------------
	
	//Note -- inherits a prevPage, curPage, & nextPage from HFPage
	//		  no need to redeclare locally
	
	PageId headBMPageId;
	
	
	//----------------
	//Constructors
	//----------------
	
	//Default Constructor
	public CBitMapHeaderPage()
		throws HFException, HFBufMgrException, IOException
	{
		//Does nothing, inherits an empty bytes[] from Page class
		super();
		//create an empty BMPage to point to
		CBMPage headPage = new CBMPage();
		headBMPageId = new PageId();
		
		//create the Page in DB
		headBMPageId = newPage(headPage, 1);
		// check error
		if(headBMPageId == null)
			throw new HFException(null, "can't new page");
		
		//initialize the page buffer
		headPage.init( headBMPageId, headPage );
		
		unpinPage(headBMPageId, true /*dirty*/ );
	}
	
	public CBitMapHeaderPage( PageId pageNo )
		throws ConstructPageException
	{
		super();
		headBMPageId = pageNo;
		try
		{
			//page already exists, so a pin Call will get it from the disk/buffer
			SystemDefs.JavabaseBM.pinPage(pageNo, this, false/*Rdisk*/); 
		}
		catch (Exception e)
		{
			throw new ConstructPageException(e, "pinpage failed");
		}
	}
	
	//initialize this Bitmap to align with the type of the columnar file column
	public void init( ValueClass value )
		throws HFBufMgrException, IOException
	{
		CBMPage headPage = new CBMPage();
		//ensure the page is in the buffer for edits
		pinPage(headBMPageId, headPage, false/*read disk*/);
		headPage.setMapType( (short)value.getType() );
		//page has been potentially dirtied with an update
		unpinPage(headBMPageId, true);
	}
	
	//----------------
	//Page Manipulation Methods
	//(mirrored from Heapfile.java)
	//----------------
	
	/**
	 * short cut to access the pinPage function in bufmgr package.
	 * @see bufmgr.pinPage
	 */
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

	/**
	 * short cut to access the unpinPage function in bufmgr package.
	 * @see bufmgr.unpinPage
	 */
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
	
	//method to create a page that will exist in the DB
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
	
	//----------------
	//Accessors
	//----------------
	
	PageId getPageId()
		throws IOException
    {
		return getCurPage();
    }
	
	//----------------
	//Functional Methods
	//----------------
	
	//Insert will do some subset of 3 things
	//	1) Check if this pageNo/slotNo are already in the BitMap File (on any page)
	//	2) If not present, look for a page with space to do the insert
	//	3) If no page has space, make a new page to insert the mapping
	public boolean insertMap( ValueClass value, short pageNo, short slotNo )
		throws HFException, HFBufMgrException, IOException
	{
		boolean successfulInsert = false;
		
		//start at head and read forward in list
		PageId currentPageId = new PageId();
		currentPageId.pid = headBMPageId.pid;
		CBMPage currentBMPage = new CBMPage();
		
		//go until reaching the end of the list
		boolean keepChecking = true;
		boolean noDupeFound = true;
		PageId firstPageWithSpace = new PageId( -1 );
		
		//check for the existence of this pair before inserting a Dupe
		//duplicate values are acceptable, but duplicate pointers to
		//the same columnarfile entry are not
		while( keepChecking )
		{
			//pin page with this PageId for use, if it is in the buffer
			//it will be pinned otherwise it will be pulled into the buffer
			pinPage(currentPageId, currentBMPage, false/*read disk*/);
			
			//check if this page has the values already stored in it
			if( -1 != currentBMPage.checkPairExistence(pageNo, slotNo) )
			{
				//found a duplicate RID
				keepChecking = false;
				noDupeFound = false;
				//page was not updated so it isn't dirty
				unpinPage(currentPageId, false /*not DIRTY*/);
			}
			else
			{
				//while in buffer, check if there is space to do this insert
				//if no RID duplicate is present, then we will proceed to insert
				//on first page with space
				if( (-1 == firstPageWithSpace.pid) && 
					currentBMPage.sufficientInsertSpace(value) )
				{
					firstPageWithSpace.pid = currentPageId.pid;
				}
				
				//go to next page in list
				PageId nextPageId = new PageId();
				nextPageId = currentBMPage.getNextPage();
				//if we reach the end of the list, exit
				if( -1 == nextPageId.pid )
				{
					keepChecking = false;
					//unpin page for now
					unpinPage(currentPageId, false /*not DIRTY*/);
					//do not iterate to next page
				}
				else
				{
					//else continue to next page
					unpinPage(currentPageId, false /*not DIRTY*/);
					currentPageId.pid = nextPageId.pid;
				}
			}
		}
		
		//if the pair being inserted isn't already in the list,
		//attempt to insert the data
		if( noDupeFound )
		{
			//a page with space was found while duplicate checking, use it
			if( -1 != firstPageWithSpace.pid )
			{
				//pull the page back into the buffer
				pinPage(firstPageWithSpace, currentBMPage, false/*read disk*/);
				
				//insert the new record
				currentBMPage.insertRecord( value, pageNo, slotNo );
				successfulInsert = true;
				
				//page was updated so it is dirty
				unpinPage(firstPageWithSpace, true /*DIRTY*/);
			}
			else //we need to allocate a new page
			{
				//pin curPAgeId, which should still be pointing to the last page in the list
				pinPage(currentPageId, currentBMPage, false/*read disk*/);
				
				//create the new page
				CBMPage freshPage = new CBMPage();
				PageId freshPageId = new PageId();
		
				//create the Page in DB
				freshPageId = newPage(freshPage, 1);
				// check error
				if(freshPageId == null)
					throw new HFException(null, "can't new page");
				
				freshPage.init( freshPageId, freshPage );
				//ensure new page has proper type field set
				freshPage.setMapType( (short)value.getType() );
				
				//link it into the list of pages
				currentBMPage.setNextPage( freshPage.getCurPage() );
				freshPage.setPrevPage( currentBMPage.getCurPage() );
				
				//insert the new mapping intothe new page
				freshPage.insertRecord( value, pageNo, slotNo );
				successfulInsert = true;
				
				//page had its pointers updated, so it is dirty
				unpinPage(currentPageId, true /*DIRTY*/);
				unpinPage(freshPageId, true /*dirty*/ );
				
			}
		}
		
		return successfulInsert;
		
	}
	
	//Remove the map pointers for a given page/slot in the columnar
	//to effectively "delete" the mapping inthe Bitmap file
	//If the pageNo/slotNo pair aren't mapped then nothing occurs
	public boolean deleteMap( short pageNo, short slotNo )
		throws HFBufMgrException, IOException,
				InvalidSlotNumberException
	{
		boolean successfulDelete = false;
		
		//start at head and read forward in list
		PageId currentPageId = headBMPageId;
		CBMPage currentBMPage = new CBMPage();
		
		//go until reaching the end of the list
		boolean keepChecking = true;
		while( keepChecking )
		{
			//pin page with this PageId for use, if it is in the buffer
			//it will be pinned otherwise it will be pulled into the buffer
			pinPage(currentPageId, currentBMPage, false/*read disk*/);
			
			//"deleteRecord" will return true if it matches the page & slot
			//number given, else it indicates the data being deleted isn't in
			//this particular page
			if( currentBMPage.deleteRecord(pageNo, slotNo) )
			{
				successfulDelete = true;
				//value was deleted, can exit
				keepChecking = false;
				//page was updated so it is dirty
				unpinPage(currentPageId, true /*DIRTY*/);
			}
			else //go to next page in list
			{
				PageId nextPageId = currentBMPage.getNextPage();
				//if we reach the end of the list, exit
				if( -1 == nextPageId.pid )
				{
					keepChecking = false;
				}
				unpinPage(currentPageId, false /*not DIRTY*/);
				currentPageId = nextPageId;
			}
		}
		
		return successfulDelete;
	}
	
}