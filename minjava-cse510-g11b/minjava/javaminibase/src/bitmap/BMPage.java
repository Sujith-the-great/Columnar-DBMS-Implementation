/*
 * File - BMPage.java
 *
 * Original Author - Jackson Nichols
 *
 * Description - 
 *		Single Page in the Bitmap File that contains a set of
 *		mapped attributes from a columnar file. The mapped attributes
 *		also store metadata to locate the page in the columnarfile
 *		it maps to.
 */
package bitmap;

import java.io.*;
import java.lang.*;
import global.*;

import diskmgr.*;
import btree.*; //for exceptions
import heap.*; //for exceptions

 /*
  * Define constant values for INVALID_SLOT and EMPTY_SLOT
  */
interface ConstPtr
{
	short INVALID_PTR =  -1;
	short EMPTY_PTR = -1;
}

/*=======================
 * BMPage Class
 *=======================
 * 
 * This class relies on the columnarfile & pages it relates to to be laid out in a manner similar to the following
 * 
 *			Heapfile Page of Column Values
 *	 _______________________________________________
 *	|				[page metadata here]			|
 *	| Slot 0: offset_ptr0 (short) && length (short)	|
 *	| Slot 1: offset_ptr1 (short) && length (short)	|
 *	| Slot 2: offset_ptr2 (short) && length (short)	|
 *	|					...							|
 *	| Record 2: column attribute (of some length)	| [starts at MAX_SPACE - offset_ptr2 in buffer]
 *	| Record 1: column attribute (of some length)	| [starts at MAX_SPACE - offset_ptr1 in buffer]
 *	| Record 0: column attribute (of some length)	| [starts at MAX_SPACE - offset_ptr0 in buffer]
 *	|_______________________________________________|
 * 
 * The Bitmap will map to that Columnar Page as follows
 *	 _______________________________________________
 *	|				[page metadata here]			|
 *	| Ptr 0: offset_ptr0 (short) && length (short)	|
 *	|		&& slot # && page # (short)				|
 *	| Ptr 1: offset_ptr0 (short) && length (short)	|
 *	|		&& slot # && page # (short)				|
 *	| Ptr 2: offset_ptr0 (short) && length (short)	|
 *	|		&& slot # && page # (short)				|
 *	|					...							|
 *	| BM 2: Bitmap of column attribute				| [starts at MAX_SPACE - offset_ptr2 in buffer]
 *	| BM 1: Bitmap of column attribute				| [starts at MAX_SPACE - offset_ptr1 in buffer]
 *	| BM 0: Bitmap of column attribute				| [starts at MAX_SPACE - offset_ptr0 in buffer]
 *	|_______________________________________________|
 *	
 * The Bitmap slots will contain the slot & page number this mapped attribute aligns to.
 * When inserting an entry into the Columnarfile Page it will be assigned some slot in some page
 * capable of holding the information. The corresponding bitmap update will be to find a bitmap page
 * with free space or an unused Ptr. It will then update the Ptr to use the slot & page number
 * of the Attribute it maps.
 * Similar to a HFPage, we will keep track of where certain maps are stored in memory through
 * offsets and lengths. All integer values will have the same length, but string inputs may
 * vary from 1-50 characters.
 * On Deletes we will remove the bitmap, compact the data that surrounded the removed map, and then
 * update the Ptr values to the new positions in memory. The Ptr to the deleted map will have INVALID
 * set for all its values to indicate it is allocated but unused.
 * 
 * -----
 * 
 * Integer Bitmapping methodology
 *
 * The BitMapping methodology for this is expected to be able to handle any inputs in the range of an Integer.
 * Rather than attempt to set-up a dynamically sizing and re-mapping methodology for the Bitmap, it will instead
 * be able to handle the scope of inputs by default.
 * This is a trade off for range scans for simplicity of the file.
 * To manage this we assert that Integers range from -2,147,483,647 to 2,147,483,647, and must thus be capable of
 * mapping 4,294,967,295 many values. We achieve this mapping using component based mapping with modulo of powers of 256
 * For an input number we treat -2,147,483,647 as the "new zero" value and translate the input as
 * 
 * 	input - (-2,147,483,647) = result
 * 
 * then we take that result and map it according to
 * 
 * 	256^3[bit] + 256^2[bit] + 256[bit] + 1[bit]
 *
 * This enables a mapping of the full range of integers into a bitmap with 4 components. Each component is a power
 * of 256 from powers 0 to 3. Then, instead of mapping the entire 256 bits in each component, we can work with the
 * fact that only 1 bit will be set in each part and compress the representation to jsut track which bit is set in
 * the range [0, 255] (or a single ubyte). That compresses the bitmap in memory down to a mere 4 bytes.
 * 
 * Combined with the size of the pointer we should require 8 bytes to store each mapping.
 * 
 * -----
 * 
 * String Bitmap methodology
 * 
 * Strings are much less easy to convert into a bitmap given the complexity of neatly storing a series of characters as
 * an bitmapping. Valid characters range 0-127 and there could be upto 50 characters in a string. The would require a map length
 * of 128 per character while needing to map each character in the string in such a way the alphabetical order is maintained
 * to support range searches.
 * The path forward taken was to represent the bitmap in a range of 0-127 per character
 * 
 * [128bits (char 1), 128bits (char 2), ... ]
 * 
 * which can be compressed to a byte to indicate which bit was set out of the 128 range as
 * 
 * [byte 1 (char 1), byte 2 (char 2), ... ]
 *
 * Effectively reducinga bitmap for "JACKSON" from
 *	[(0-64) A B C D E F G H I J K L M N O P Q R S T U V W X Y Z (91-127) ]
 *	[ ...   0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  ...     ]
 *	[ ...   1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  ...     ]
 *	[ ...   0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  ...     ]
 *	[ ...   0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  ...     ]
 *	[ ...   0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0  ...     ]
 *	[ ...   0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0  ...     ]
 *	[ ...   0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0  ...     ]
 * (a 7x128 entry table; 112 bytes in memory)
 *  Down to
 *	[74 65 67 75 83 79 78]
 * (a 1x7 entry table; 7 bytes in memory)
 */
public class BMPage extends Page
					implements ConstPtr
{
	//----------------
	//Class Variables
	//----------------
	
	//Helper constants (in bytes)
	private static final int SIZEOF_BYTE = 1;
	private static final int SIZEOF_SHORT = 2;
	private static final int SIZEOF_INT = 4;
	private static final int INT_MIN = -2147483647;
	
	//Size of each pointer set (the slot # & page # of the mapped attribute)
	public static final int MAP_START_PTR = 0;
	public static final int MAP_LENGTH 	= MAP_START_PTR + SIZEOF_SHORT;
	public static final int PAGE_PTR 	= MAP_LENGTH + SIZEOF_SHORT;
	public static final int SLOT_PTR 	= PAGE_PTR + SIZEOF_SHORT;
	public static final int SIZE_OF_PTR = SIZEOF_SHORT * 4;
	//Size of each bitmapping (the 4 bytes to hold the map)
	public static final int SIZE_OF_INTEGER_MAP = SIZEOF_BYTE * 4;
	
	/*
	 * Notice, the following are computed as the offsets of the metadata in the page
	 *	 _______________________________________________________________________________________________
	 *	|	MAP_COUNT	| MAP_TYPE	| NEXT_EMPTY	| FREE_SPACE	| PREV_PAGE	| NEXT_PAGE	| CUR_PAGE	|
	 *	|_______________________________________________________________________________________________|
	 *	^Buffer Start
	 */
	public static final int START_OF_BUFFER = 0;
	public static final int MAP_COUNT = START_OF_BUFFER; 			//MAP_COUNT is a short
	public static final int MAP_TYPE  = MAP_COUNT  + SIZEOF_SHORT;	//MAP_TYPE  is a short
	public static final int NEXT_EMPTY= MAP_TYPE   + SIZEOF_SHORT; 	//NEXT_EMPTY is a short
	public static final int FREE_SPACE= NEXT_EMPTY + SIZEOF_SHORT; 	//FREE_SPACE is a short
	public static final int PREV_PAGE = FREE_SPACE + SIZEOF_SHORT; 	//PREV_PAGE is an int
	public static final int NEXT_PAGE = PREV_PAGE + SIZEOF_INT; 	//NEXT_PAGE is an int
	public static final int CUR_PAGE  = NEXT_PAGE + SIZEOF_INT; 	//CUR_PAGE  is an int
	//size of page metadata fields
	//sizeof(MAP_COUNT) + sizeof(MAP_TYPE) + sizeof(NEXT_EMPTY) + sizeof(FREE_SPACE) + sizeof(PREV_PAGE) + sizeof(NEXT_PAGE) + sizeof(CUR_PAGE)
	public static final int METADATA_SIZE = 4*SIZEOF_SHORT + 3*SIZEOF_INT;
	
	//number of slots in use
	private    short     mapCnt;
	//AttrType of the data in this bitmap (int or string)
	private    short     mapType;
	//offset of first used byte by data records in data[]
	private    short     emptyPtr;
	//number of bytes free in data[]
	private    short     freeSpace;
	//backward pointer to data page
	private    PageId    prevPage = new PageId();
	//forward pointer to data page
	private    PageId    nextPage = new PageId();
	//page number of this page
	protected  PageId    curPage  = new PageId();
	
	//----------------
	//Constructors
	//----------------
	
	//Default Constructor
	public BMPage()
	{
		//Does nothing unique here
		//creates an empty data[] from Page constructor
		super();
	}
	
	//Constructor of class BMPage open a BMPage and
	//make this BMPage point to the given page
	public BMPage( Page page )
	{
		//take data from other page and store in this page
		data = page.getpage();
	}
	
	//Constructor of class BMPage initialize a new page
	//it is expected the page be init() after construction for complete set-up
	//this method will also override a page with data if passed in with content
	public void init(PageId pageNo, Page apage)
		throws IOException
	{
		//take data from other page and store in this page
		data = apage.getpage();
		
		//indicate all slots in this new page are unused
		mapCnt = 0;
		Convert.setShortValue(mapCnt, MAP_COUNT, data);
		
		//default all maps to int tracking, differences have another method to update it
		mapType = AttrType.attrInteger;
		Convert.setShortValue(mapType, MAP_TYPE, data);
		
		//start location of the free space in the buffer (we put the bitmaps at
		//the end of the buffer and grow towards the slots)
		emptyPtr = (short) MAX_SPACE;
		Convert.setShortValue(emptyPtr, NEXT_EMPTY, data);
		
		//space avaialbe in this page
		freeSpace = (short) (MAX_SPACE - METADATA_SIZE);
		Convert.setShortValue(freeSpace, FREE_SPACE, data);
		
		//initialize the Page IDs of the previous & next page to -1
		nextPage.pid = prevPage.pid = INVALID_PAGE;
		Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
		Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
		
		//Assign the Page ID of the passed PageID object to this BMPage
		curPage.pid = pageNo.pid;
		Convert.setIntValue(curPage.pid, CUR_PAGE, data);
	}

	//Constructor of class BMPage open an existing BMPage
	public void openBMpage(Page apage)
	{
		//take data from other page and store in this page
		data = apage.getpage();
	}
	
	//----------------
	//Page Navigation Methods
	//----------------
	
	//get value of nextPage
	public PageId getCurPage()
		throws IOException
    {
		curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
		return curPage;
    }
	
	//sets value of nextPage to pageNo
	public void setCurPage(PageId pageNo)   
		throws IOException
    {
		curPage.pid = pageNo.pid;
		Convert.setIntValue (curPage.pid, CUR_PAGE, data);
    }

	//get value of nextPage
	public PageId getNextPage()
		throws IOException
    {
		nextPage.pid =  Convert.getIntValue(NEXT_PAGE, data);    
		return nextPage;
    }

	//sets value of nextPage to pageNo
	public void setNextPage(PageId pageNo)
		throws IOException
    {
		nextPage.pid = pageNo.pid;
		Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

	//get value of prevPage
	public PageId getPrevPage()
		throws IOException 
    {
		prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
		return prevPage;
    }

	//sets value of prevPage to pageNo
	public void setPrevPage(PageId pageNo)
		throws IOException
    {
		prevPage.pid = pageNo.pid;
		Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }
	
	//----------------
	// ValueClass Type Methods
	//----------------
	
	//get the type of attribute mapped in this page
	public short getMapType()
		throws IOException
	{
		mapType = Convert.getShortValue(MAP_TYPE, data);
		return mapType;
	}
	
	//update the type specifier for this bitmap
	public void setMapType( short type )
		throws IOException
	{
		mapType = type;
		Convert.setShortValue(mapType, MAP_TYPE, data);
	}
	
	//----------------
	//Bitmap Slot Retrieval Methods
	//----------------
	
	//set the values in a PTR entry to the appropriate page & slot
	public void setPtr(int ptrNo, short mapStart, short mapLength, short pageNo, short slotNo)
		throws IOException
    {
		int position = METADATA_SIZE + (ptrNo * SIZE_OF_PTR);
		Convert.setShortValue(mapStart, position + MAP_START_PTR, data);
		Convert.setShortValue(mapLength, position + MAP_LENGTH, data);
		Convert.setShortValue(pageNo, position + PAGE_PTR, data);
		Convert.setShortValue(slotNo, position + SLOT_PTR, data);
    }
	
	//get the starting offset of this map
	public short getPtrMapStart(int ptrNo)
		throws IOException
    {
		int position = METADATA_SIZE + (ptrNo * SIZE_OF_PTR) + MAP_START_PTR;
		short val= Convert.getShortValue(position, data);
		return val;
    }
	
	//get the length of this mapping
	public short getPtrMapLength(int ptrNo)
		throws IOException
    {
		int position = METADATA_SIZE + (ptrNo * SIZE_OF_PTR) + MAP_LENGTH;
		short val= Convert.getShortValue(position, data);
		return val;
    }
	
	//get the Page the mapping is pointing to
	//this is the pointer to the PageId containing the data
	public short getPtrPage(int ptrNo)
		throws IOException
    {
		int position = METADATA_SIZE + (ptrNo * SIZE_OF_PTR) + PAGE_PTR;
		short val= Convert.getShortValue(position, data);
		return val;
    }
	
	//get the Slot in the Page the mapping is pointing to
	//this pointer is to the slot in the Heapfile page
	public short getPtrSlot(int ptrNo)
		throws IOException
    {
		int position = METADATA_SIZE + (ptrNo * SIZE_OF_PTR) + SLOT_PTR;
		short val= Convert.getShortValue(position, data);
		return val;
    }
	
	//checks the page to see if it already maps a given value
	//at a passed page/slot number & returns the ptr count
	//if it exists, else returns -1
	public int checkPairExistence( short pageNo, short slotNo )
		throws IOException
	{
		int indexOfMatch = INVALID_PTR;
		mapCnt = Convert.getShortValue(MAP_COUNT, data);
		
		// first check if the record being deleted is actually valid
		if ( (pageNo >= 0) && (slotNo >= 0)  )
		{
			//iterate the ptr entries on this page for a match
			for( int i = 0; i < mapCnt; i++ )
			{
				//check if this ptr is a match on the page and slot being deleted
				if( (slotNo == getPtrSlot(i)) && (pageNo == getPtrPage(i)) )
				{
					indexOfMatch = i;
					//quick exit
					i = mapCnt;
				}
			}
		}
		
		return indexOfMatch;
	}
	
	//----------------
	//Support Methods
	//----------------
	
	//get the map count in this page (valid and invalid)
	public short getMapCnt() 
		throws IOException
    {
		mapCnt = Convert.getShortValue(MAP_COUNT, data);
		return mapCnt;
    }
	
	//returns the bytes at a given mapping offset
	public byte[] getMapAtSlot(int ptrNo)
		throws IOException
	{
		short startAddr = getPtrMapStart( ptrNo );
		short dataLen = getPtrMapLength( ptrNo );
		byte[] map = new byte[dataLen];
		System.arraycopy(data, startAddr, map, 0, dataLen);
		return map;
	}
	
	//support method to check bitmaps in this page and return
	//all entries numbers containing matching values
	//returns the ptrs with matches, which can be used to get Page/Slot
	//entries from the columnar file
	public int[] matchValueInMap( ValueClass valToMatch )
		throws IOException
	{
		//define holding variables for
		byte[] mapCheck;
		if( AttrType.attrString == valToMatch.getType() )
		{
			String tmpString = Convert.getStrValue( 0, valToMatch.getClassValue(), valToMatch.getValueLength() );
			mapCheck = convertStringToMap( tmpString );
		}
		else
		{
			int tmpInt =  Convert.getIntValue( 0, valToMatch.getClassValue() );
			mapCheck = convertIntToMap( tmpInt );
		}
		
		//iterate all maps in this page
		mapCnt = Convert.getShortValue(MAP_COUNT, data);
		//assume worst case we could return every mapping in this page
		int[] matches = new int[mapCnt+1];
		int matchCount = 0;
		for (int i= 0; i < mapCnt; i++) 
		{
			//compare the 2 maps
			if( mapCheck == getMapAtSlot(i) )
			{
				//track the indices that had matches
				matches[matchCount+1] = i;
				matchCount++;
			}
		}
		//set the first index to the number of matched mappings found
		matches[0] = matchCount;
		
		return matches;
	}
	
	//convert an input string into a bitmapping
	public byte[] convertStringToMap( String input )
	{
		byte[] conversion = input.getBytes();
		return conversion;
	}
	
	//convert a mapped value back to a string
	public String convertMapToString( byte[] input )
	{
		String unmapped = new String(input);
		return unmapped;
	}
	
	//convert an integer into a component bitmapping of 4 powers of 256
	public byte[] convertIntToMap( int input )
	{
		byte[] conversion = new byte[SIZE_OF_INTEGER_MAP];
		//need to use a long to get unsigned length of an integer
		long operatedValue = input;
		//subtract the INT_MIN to treat INT_MIN as the "new 0"
		//and preserve order even with negatives
		operatedValue -= INT_MIN;
		//pre-compute 256^3
		int power256 = 16777216;
		
		//256 will take 4 components [0,3] to map the full range of integers
		//we will map the highest power of 256 in the Most Significant bit
		for(int component = (SIZE_OF_INTEGER_MAP-1); component >= 0; component-- )
		{
			//calcualte how many times this power of 256 evenly divides the input
			int remainder = (int)(operatedValue / power256);
			//should never exceed 256, but modulo for safety
			byte bit = (byte)(remainder % 256);
			//store the specific set bit in the conversion byte[]
			conversion[3 - component] = bit;
			//subtract the value stored in that byte from the value being stored
			operatedValue -= remainder * power256;
			//go to the next power of 256 for the next component
			power256 /= 256;
		}
		
		return conversion;
	}
	
	//convert a mapped value back to an int
	public int convertMapToInt( byte[] input )
	{
		//because java doesn't naturally support uints, and any positive
		//result will temporarily exceed the max of a signed int, we store
		//the conversion in a long
		long computeResult = 0;
		int result = 0;
		int power256 = 1;
		
		//start the conversion with the least significant bit (256^0) and
		//move to most significant (256^3)
		for( int i = (SIZE_OF_INTEGER_MAP-1); i >= 0; i--)
		{
			//do a bitwise & with 0xFF to convert the byte into a ubyte
			int ubyteValue = input[i] & 0xFF;
			//multiply by the set bit index by the power of 256 to get the value to add
			computeResult += power256 * ubyteValue;
			
			power256 *= 256;
		}
		//recall the original value had "-= INT_MIN" done to it before storage
		//in the byte, so we must undo that operation
		result = (int)(computeResult + INT_MIN);
		
		return result;
	}
	
	//----------------
	//Functional Methods
	//----------------
	
	//Insert a Mapping for a given attribute if there is sufficient
	//space in this BMPage to allocate the sized mapping
	//returns the ptr entry where the mapping was stored
	public int insertRecord ( ValueClass value, short pageNo, short slotNo )
		throws IOException
    {
		//initiate to failure and update return on successful
		//insertion (failure is -1)
		int ptrSlotInserted = INVALID_PTR;
		
		int recordSize = 0;
		//Based on value class, get the size of the record needed to map
		switch( value.getType() )
		{
			case AttrType.attrString:
					String tmpString = Convert.getStrValue( 0, value.getClassValue(), value.getValueLength() );
					recordSize = tmpString.length();
				break;
				
			//make the Integer case the default un unknown inputs
			case AttrType.attrInteger: 
			default:
					recordSize = SIZE_OF_INTEGER_MAP;
				break;
			
		}
		
		// Check if sufficient space exists for a new map
		int spaceNeeded = recordSize + SIZE_OF_PTR;
		freeSpace = Convert.getShortValue(FREE_SPACE, data);
		//continue if there is sufficient space to allocate if needed
		if (spaceNeeded <= freeSpace)
		{
			// look for an invalid ptr entry to reuse
			mapCnt = Convert.getShortValue(MAP_COUNT, data);
			int ptrLocation = INVALID_PTR;
			for (int i= 0; i < mapCnt; i++) 
			{
				if (INVALID_PTR == getPtrPage(i) )
				{
					//quick exit on found unused ptr
					ptrLocation = i;
					i = mapCnt;
				}
			}
			
			//all allocated ptrs on the page are in use,
			//attempt to allocate a new one
			if(INVALID_PTR == ptrLocation)
			{
				// adjust free space
				freeSpace -= spaceNeeded;
				Convert.setShortValue (freeSpace, FREE_SPACE, data);
				
				//set the ptrLocation to the tip of the mapCnt
				ptrLocation = mapCnt;
				//increment mapCnt on new allocation
				mapCnt++;
				Convert.setShortValue (mapCnt, MAP_COUNT, data);
			}
			else //reuse a ptr entry no longer pointing to valid entry
			{
				// adjust free space
				freeSpace -= recordSize;
				Convert.setShortValue (freeSpace, FREE_SPACE, data);
			}
			
			//adjust the location the "nextEmpty" pointer is in the buffer
			emptyPtr = Convert.getShortValue(NEXT_EMPTY, data);
			emptyPtr -= recordSize;
			Convert.setShortValue (emptyPtr, NEXT_EMPTY, data);
			
			//insert the ptr info onto the data page
			//note we use emptyPtr at this point (after subtracting size) because
			//the records grow from the end of the buffer to start
			setPtr(ptrLocation, emptyPtr, (short)recordSize, pageNo, slotNo);
			//update insertion location
			ptrSlotInserted = ptrLocation;
			
			//update the bitmap determing on type of mapping occuring
			if( AttrType.attrString == getMapType() )
			{
				String someStringToConvert = Convert.getStrValue( 0, value.getClassValue(), value.getValueLength() );
				byte[] newMap = new byte[recordSize];
				newMap = convertStringToMap( someStringToConvert );
				System.arraycopy(newMap, 0, data, emptyPtr, recordSize);
			}
			else
			{
				int someIntToConvert = Convert.getIntValue( 0, value.getClassValue() );
				byte[] newMap = new byte[recordSize];
				newMap = convertIntToMap( someIntToConvert );
				System.arraycopy(newMap, 0, data, emptyPtr, recordSize);
			}
		}
		//else we cannot allocate on this page
		
		return ptrSlotInserted;
    } 
  
	//"Delete" removes mapped bytes fro mthe buffers, invalidates
	//the slot pointing to that mapping, shifts the remaining bytes
	// in memory over to stay compact, & then updates pointers
	//to those maps
	public boolean deleteRecord( short pageNo, short slotNo )
		throws IOException, InvalidSlotNumberException
    {
		boolean successfulDelete = false;
		mapCnt = Convert.getShortValue(MAP_COUNT, data);
		
		// first check if the record being deleted is actually valid
		int indexToDelete = checkPairExistence( pageNo, slotNo );
		if( INVALID_PTR != indexToDelete )
		{
			//shift the data from the deleted map by the size of the deleted map
			//in order to preserve compacted page content
			short deleteStart = getPtrMapStart( indexToDelete );
			short deleteSize = getPtrMapLength( indexToDelete );
			short sizeToShift = (short)(deleteStart - emptyPtr);
			short newEmptyPtr = (short)(emptyPtr + deleteSize);
			System.arraycopy(data, emptyPtr, data, newEmptyPtr, sizeToShift);
			//update the empty pointer after this shift
			emptyPtr = newEmptyPtr;
			Convert.setShortValue(emptyPtr, NEXT_EMPTY, data);
			
			//now iterate all affected pointers and shift their start points accordingly
			for( short i = 0; i < mapCnt; i++ )
			{
				//pointers aren't necessarily in order relative to the data they
				//map in the page, so we need to identify which records were affected
				//by the above data shift
				//ie, if we delete 5 bytes of map, then this map starts 5 bytes higher in
				//the buffer, but only if it was lower in the buffer than the deleted map
				short thisMapStart = getPtrMapStart(i);
				if( thisMapStart > deleteStart )
				{
					short newStart = (short)(thisMapStart + deleteSize);
					setPtr( i, newStart, getPtrMapLength(i), getPtrPage(i), getPtrSlot(i) );
				}
			}
			
			// adjust free space
			freeSpace += deleteSize;
			Convert.setShortValue (freeSpace, FREE_SPACE, data);
			
			//Now we set this particular index to be unclaimed/invalid
			setPtr(indexToDelete, INVALID_PTR, INVALID_PTR, INVALID_PTR, INVALID_PTR);
			successfulDelete = true;
		}
		return successfulDelete;
    }
	
	//support method to do a size check
	public boolean sufficientInsertSpace( ValueClass value )
		throws IOException
	{
		boolean canInsert = false;
		
		int recordSize = 0;
		//Based on value class, get the size of the record needed to map
		switch( value.getType() )
		{
			case AttrType.attrString:
					String tmpString = Convert.getStrValue( 0, value.getClassValue(), value.getValueLength() );
					recordSize = tmpString.length();
				break;
				
			//make the Integer case the default un unknown inputs
			case AttrType.attrInteger: 
			default:
					recordSize = SIZE_OF_INTEGER_MAP;
				break;
			
		}
		
		// Check if sufficient space exists for a new map
		int spaceNeeded = recordSize + SIZE_OF_PTR;
		freeSpace = Convert.getShortValue(FREE_SPACE, data);
		//continue if there is sufficient space to allocate if needed
		if (spaceNeeded <= freeSpace)
		{
			canInsert = true;
		}
		
		return canInsert;
	}
	
	//returns the amount of available space on the page.
	public int available_space()
		throws IOException
    {
		freeSpace = Convert.getShortValue(FREE_SPACE, data);
		return freeSpace;
    }
	
	//Dump contents of a page in a formatted style
	public void dumpPage()
		throws IOException
	{
		//update these 3 values as part of the data dump
		emptyPtr = Convert.getShortValue(NEXT_EMPTY, data);
		freeSpace = Convert.getShortValue(FREE_SPACE, data);
		mapCnt = Convert.getShortValue(MAP_COUNT, data);
		
		System.out.println("Bitmap Page dump");
		System.out.println("curPage = " + Convert.getIntValue(CUR_PAGE, data) );
		System.out.println("nextPage = " + Convert.getIntValue(NEXT_PAGE, data));
		System.out.println("Next Empty byte= " + emptyPtr);
		System.out.println("freeSpace = " + freeSpace);
		System.out.println("Map Count = " + mapCnt);
		
		//display each mapping in this page consecutively & its pointer values
		for( int mapping = 0; mapping < mapCnt; mapping++ )
		{
			int pageNo = getPtrPage(mapping);
			int slotNo = getPtrSlot(mapping);
			System.out.print("Entry " + mapping + " maps to slot " + slotNo + " on page " + pageNo + " in the Column Heapfiie and has value " );
			if( AttrType.attrString == getMapType() )
			{
				String mappedValue = convertMapToString( getMapAtSlot(mapping) );
				System.out.println(mappedValue);
			}
			else if( AttrType.attrInteger == getMapType() )
			{
				int mappedValue = convertMapToInt( getMapAtSlot(mapping) );
				System.out.println(mappedValue);
			}
		}
	}

	//Determining if the page is empty
	public boolean empty()
		throws IOException
    {
		boolean pageIsEmpty = true;
		//get the count of maps available
		mapCnt = Convert.getShortValue(MAP_COUNT, data);
		
		//iterate the maps, if all allocated maps are
		//invalid then the page is empty
		for(int i= 0; i < mapCnt; i++) 
		{
			if (INVALID_PTR != getPtrPage(i))
			{
				pageIsEmpty = false;
				//quick exit loop
				i = mapCnt;
			}
		}		
		return pageIsEmpty;
    }

	public byte[] getBMpageArray()
	{
		return data;
	}

	//copy an input byte array into the page bytes
	public void writeBMPageArray(byte[] inputBytes)
	{
		//we do not allow copies of buffers of mismatched size
		//as the resulting behavior could be undefined
		if( inputBytes.length == MAX_SPACE )
		{
			System.arraycopy(inputBytes, 0, data, 0, MAX_SPACE);
		}
	}
	
	
}