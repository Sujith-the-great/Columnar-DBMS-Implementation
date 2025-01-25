/*
 * File - BM.java
 *
 * Original Author - Jackson Nichols
 *
 * Description - 
 *		...
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
 * BM Class
 * It implements the GlobalConst class to inherit the constant values
 */
public class BM implements GlobalConst
{
	//----------------
	//Class Variables
	//----------------
	
	//----------------
	//Constructors
	//----------------
	public BM()
	{
		//Do nothing. this class is for DEBUG prints of the BitMap
	}
	
	//----------------
	//Accessor Methods
	//----------------
	
	//----------------
	//Page Manipulation Methods
	//(mirrored from Heapfile.java)
	//----------------
	
	/**
	 * short cut to access the pinPage function in bufmgr package.
	 * @see bufmgr.pinPage
	 */
	private static void pinPage(PageId pageno, Page page, boolean emptyPage)
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
	private static void unpinPage(PageId pageno, boolean dirty)
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
	
	//----------------
	//Functional Methods
	//----------------
	
	//printBitmap is a debug method that will iterate the bitmap
	//pages of the bitmap and print context
	public static void printBitMap( BitMapHeaderPage header )
		throws HFBufMgrException, IOException
	{
		//start at head and read forward in list
		PageId currentPageId = header.getPageId();
		BMPage currentBMPage = new BMPage();
		
		//go until reaching the end of the list
		boolean keepPrinting = true;
		
		//loop all pages
		while( keepPrinting )
		{
			//pin page with this PageId for use, if it is in the buffer
			//it will be pinned otherwise it will be pulled into the buffer
			pinPage(currentPageId, currentBMPage, false/*read disk*/);
			
			//print the data on this page
			currentBMPage.dumpPage();
			//go to next page in list
			PageId nextPageId = currentBMPage.getNextPage();
			//if we reach the end of the list, exit
			if( -1 == nextPageId.pid )
			{
				keepPrinting = false;
			}
			unpinPage(currentPageId, false /*not DIRTY*/);
			currentPageId = nextPageId;
		}
		
	}
}