
//Create pcounter file as instructed in the Project 1 directions
//code comments added by developer: Jackson Nichols

package diskmgr;
public class PCounter
{
	//initialize static counter member variables
	public static int rcounter;
	public static int wcounter;
	
	//define a "reset" method for the counters
	public static void initialize()
	{
		rcounter = 0;
		wcounter = 0;
	}

	//create an incrementor for reads
	public static void readIncrement()
	{
		rcounter++;
	}

	//create a increment for writes
	public static void writeIncrement()
	{
		wcounter++;
	}

	//getters for the read & write counts
	public static int getReadCount()
	{
		return rcounter;
	}
	public static int getWriteCount()
	{
		return wcounter;
	}
}
