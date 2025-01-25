package tests;

import columnar.*;
import diskmgr.PCounter;
import global.*;
import heap.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;


public class BatchInsert {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
            return;
        }

        // Extracting command line arguments
        String dataFileName = args[0];
        String columnDBName = args[1]+".minibase-db";
        String columnarFileName = args[2];
        int numColumns = Integer.parseInt(args[3]);

        // Get the current working directory
        String currentDirectory = System.getProperty("user.dir");
        
        File file = new File(currentDirectory, columnDBName);

        // Displaying the extracted arguments
        System.out.println("Data File Name: " + dataFileName);
        System.out.println("Column DB Name: " + columnDBName);
        System.out.println("Columnar File Name: " + columnarFileName);
        System.out.println("Number of Columns: " + numColumns);
        try {
            if (file.exists()) {
                System.out.println("DB exists. Opening the DB");
                SystemDefs.MINIBASE_RESTART_FLAG = true;
            } else {
                System.out.println("DB doesn't exists, Creating new DB.");
                SystemDefs.MINIBASE_RESTART_FLAG = false;
            }
            SystemDefs sysdef = new SystemDefs( columnDBName, 10000, GlobalConst.NUMBUF,"Clock");
        } catch (Exception e) {
           
            // SystemDefs.MINIBASE_RESTART_FLAG = false;
            // SystemDefs sysdef = new SystemDefs( columnDBName, 10000, GlobalConst.NUMBUF,"Clock"); 
            // TODO: handle exception
        }

        try (BufferedReader br = new BufferedReader(new FileReader(dataFileName))) {
			
			
            String line;
            // Read the first line for attribute names and types
            String[] attributeInfo = br.readLine().split("\\s+");

            // Initialize attribute types array
            AttrType[] attributeTypes = new AttrType[numColumns];
            String[] columnNames = new String[numColumns];

            int sSize = 0;
            // Parse attribute types from the first line of the data file
            for (int i = 0; i < numColumns; i++) {
				
				
                String[] attrInfo = attributeInfo[i].split(":");

                // Validate attribute info format
                if (attrInfo.length != 2) {
                    // Handle error if attribute info format is incorrect
                    throw new IllegalArgumentException("Invalid attribute info format: " + attributeInfo[i]);
                }

                 // Extract attribute name and type
                columnNames[i] = attrInfo[0];
                String attributeType = attrInfo[1];

                switch (attributeType) {
                    case "int":
                        attributeTypes[i] = new AttrType(AttrType.attrInteger);
                        break;
                    case "float":
                        attributeTypes[i] = new AttrType(AttrType.attrReal);
                        break;
                    case "string":
                        attributeTypes[i] = new AttrType(AttrType.attrString);
                        sSize++;
                        break;
                    default:
                        attributeTypes[i] = new AttrType(AttrType.attrString);
                        sSize++;
                        break;
                    // Handle other attribute types as needed
                }
            }
			

            // Initialize array to store maximum string sizes for each column
            short[] strSizes = null;
            if (sSize != 0){
                strSizes = new short[sSize];
                // Set all attribute sizes to the value of MAX_NAME
                for (int i = 0; i < strSizes.length; i++) {
                    strSizes[i] = (short) GlobalConst.MAX_NAME;
                }
            }
			
            
            Columnarfile columnarFile = null;
            try {
                columnarFile = new Columnarfile(columnarFileName);
                System.out.println("Columnar File "+ columnarFileName + " exist. Opening the file...");
            } catch (Exception e) {
                System.out.println("Columnar File " + columnarFileName + " does not exist. Creating a new one...");
                try {
                    columnarFile = new Columnarfile(columnarFileName, numColumns, attributeTypes, strSizes, columnNames);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            // Read data from the file and insert into the columnar file
            int recordCount = 0;
            while ((line = br.readLine()) != null) {
				
                // Split the line into values using a delimiter (e.g., space)
                String[] values = line.split("\\s+");
                
                // Create a new tuple
                Tuple tuple = new Tuple();
                try {
                    // Set the header for the tuple
                    tuple.setHdr((short) numColumns, attributeTypes, strSizes);
 
                    // Set field values based on attribute types
                    for (int i = 0; i < numColumns; i++) {
                        switch (attributeTypes[i].attrType) {
                            case AttrType.attrInteger:
                                tuple.setIntFld(i + 1, Integer.parseInt(values[i]));
                                break;
                            case AttrType.attrReal:
                                tuple.setFloFld(i + 1, Float.parseFloat(values[i]));
                                break;
                            case AttrType.attrString:
                                tuple.setStrFld(i + 1, values[i]);
                                break;
                            default:
                                tuple.setStrFld(i + 1, values[i]);
                                break;       
                        }
                    }
                    
                    // Insert the tuple into the columnar file
                    columnarFile.insertTuple(tuple.getTupleByteArray());
                    recordCount++;
                    
                    if(recordCount%1000 == 0){
                        System.out.println(recordCount + " Inserted...");
                    }
                } catch (Exception e) {
                    // Handle any errors that occur during insertion
                    e.printStackTrace();
                }
            }
            
            SystemDefs.JavabaseBM.flushAllPages();
            System.out.println("Batch Insert Read Count:- "+PCounter.rcounter);
            System.out.println("Batch Insert Write Count:- "+PCounter.wcounter);
            System.out.println("Tuple Count:- "+ columnarFile.getTupleCnt());
           
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
