package tests;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrType;
import global.FloatValueClass;
import global.GlobalConst;
import global.IntegerValueClass;
import global.StringValueClass;
import global.SystemDefs;
import global.ValueClass;


/**
 * This class is responsible for creating indexes (either B+ tree or bitmap) on a specified column of a columnar file.
 */
public class Index {

    public static AttrType keyType; // The type of the column key
    public static int columnIndex; // The index of the column
    public static ValueClass valueClass; // The value class associated with the column type

    /**
     * Finds the index of the specified column in the columnar file and determines the corresponding value class.
     *
     * @param columnarFile The columnar file containing the column.
     * @param columnName   The name of the column to search for.
     */
    public static void findColumnIndex(Columnarfile columnarFile, String columnName) {
        // Find the index of the column name in the columnNames array
        columnIndex = -1;
        String[] columnNames = columnarFile.getColumnName();
        AttrType[] type = columnarFile.getColumnTypes();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnarFile.columnNames[i].equals(columnName)) {
                columnIndex = i+1;
                keyType = type[i];
                break;
            }
        }

        // Determine the value class based on the column type
        if (keyType.attrType == AttrType.attrString) {
            valueClass = new StringValueClass();
        } else if (keyType.attrType == AttrType.attrReal) {
            valueClass = new FloatValueClass();
        } else if (keyType.attrType == AttrType.attrInteger) {
            valueClass = new IntegerValueClass();
        } else {
            valueClass = null;
        }
    }

    /**
     * Main method to execute the index creation process.
     *
     * @param args Command-line arguments: COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE
     */
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE");
            return;
        }

        // Extracting command line arguments
        String columnDBName = args[0];
        String columnarFileName = args[1];
        String columnName = args[2];
        String indexType = args[3];

        // Displaying the extracted arguments
        System.out.println("Column DB Name: " + columnDBName);
        System.out.println("Columnar File Name: " + columnarFileName);
        System.out.println("Column Name: " + columnName);
        System.out.println("Index Type: " + indexType);

        try {
            System.out.println("Open the DB");
            SystemDefs.MINIBASE_RESTART_FLAG = true;
            SystemDefs sysdef = new SystemDefs( columnDBName+".minibase-db", 10000, GlobalConst.NUMBUF,"Clock"); 
        } catch (Exception e) {
            System.out.println("DB does not exists.");
            e.printStackTrace();
            return;
        }
        
        Columnarfile columnarFile = null;
        try {
            columnarFile = new Columnarfile(columnarFileName);
            System.out.println("Columnar File "+ columnarFileName + " exist. Opening the file...");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Columnar File " + columnarFileName + " does not exist.");
            return;
        }

        // Find the index of the specified column and determine the corresponding value class
        findColumnIndex(columnarFile, columnName);

        System.out.println("Index of" + columnName + " is "+columnIndex);
        // Check if the column exists
        if (columnIndex == -1) {
            System.err.println("Column Doesn't Exist. Provide Column Name which in the table.");
            return;
        }

        try {
            // Check the index type and create the corresponding index
            boolean indexCreated = false;
            if (indexType.equals("BTREE")) {
                indexCreated = columnarFile.createBTreeIndex(columnIndex);
            } else if (indexType.equals("BITMAP")) {
                indexCreated = columnarFile.createBitMapIndex(columnIndex, valueClass);
            }

            // Print whether the index was successfully created or not
            if (indexCreated) {
                System.err.println("Index created successfully.");
            } else {
                System.err.println("Index creation failed.");
            }
            
            SystemDefs.JavabaseBM.flushAllPages();
            System.out.println("Batch Insert Read Count:- "+PCounter.rcounter);
            System.out.println("Batch Insert Write Count:- "+PCounter.wcounter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
