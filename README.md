# Columnar DBMS Implementation

This repository contains the code, documentation, and resources for implementing a columnar database management system (DBMS) as part of the CSE 510 course. The project focuses on transforming a row-based relational database (Minibase) into a columnar DBMS, introducing new features such as bitmap indexing, columnar file storage, and optimized query processing.

## Key Features

- **Columnar File Storage**: Implements a columnar storage format where each column is stored in a separate heap file, improving performance for column-based queries.
- **Bitmap Indexing**: Introduces bitmap indexing for efficient querying and filtering of data based on specific column values.
- **Query Optimization**: Implements various query access methods, including file scan, column scan, B-tree scan, and bitmap scan, to optimize query performance.
- **Batch Insertion**: Supports batch insertion of records into the columnar database, reducing the overhead of individual insert operations.
- **Query and Delete Operations**: Provides functionality for querying and deleting records based on specified conditions, with support for purging deleted records.

## Repository Main elements

- **Code**: Contains the Java implementation of the columnar DBMS, including classes for columnar file storage, bitmap indexing, and query processing.
- **Documentation**: Includes project reports, UML diagrams, and state machine diagrams.
- **Results**: Contains the results of various performance tests, including disk page read/write counts for different operations.
- **Appendices**: Includes sample test data, query examples, and a list of tools used.

## Usage

### 1. Clone the Repository
```
git clone https://github.com/your-username/Columnar-DBMS-Implementation.git
cd Columnar-DBMS-Implementation
```
## Key Components
### 1. Columnar File Storage
**ColumnarFile Class**: Manages columnar data storage, with each column stored in a separate heap file.

**TupleScan Class**: Scans tuples in the columnar file, mirroring the functionality of the row-based Scan class.

### 2. Bitmap Indexing
**BitmapFile Class**: Implements bitmap indexing for efficient querying and filtering of data.

**BMPage Class**: Manages individual pages of the bitmap index, storing mappings between values and their locations in the columnar file.

### 3. Query Processing
**ColumnFileScan Class**: Implements file scan access for querying columnar data.

**ColumnIndexScan Class**: Implements index scan access using B-tree and bitmap indexes.

**Query and Query Delete Programs**: Provide functionality for querying and deleting records based on specified conditions.

### 4. Batch Insertion
BatchInsert Program: Facilitates the insertion of multiple records into the columnar database with a single command.

## Simulation Experiments
The project includes several experiments to analyze the performance of the columnar DBMS:

**Batch Insertion**: Measures the number of disk page reads and writes for inserting large datasets.

**Index Creation**: Compares the performance of B-tree and bitmap index creation.

**Query Execution**: Evaluates the performance of different query access methods (file scan, column scan, B-tree scan, and bitmap scan).

**Query Delete**: Measures the impact of deleting records and purging deleted tuples on disk page reads and writes.

## Results
### Key Findings:
**Batch Insertion**: The batch insertion program shows a linear increase in disk page writes, but exponential growth in disk page reads due to the need to find free space in heap files.

**Index Creation**: B-tree index creation is more efficient in terms of disk page reads compared to bitmap indexing, but bitmap indexing offers faster query performance for equality searches.

**Query Execution**: Column scan access performs fewer disk page reads compared to file scan and B-tree scan, making it more efficient for column-based queries.

**Query Delete**: Deleting records and purging the database increases disk page writes, especially when using index-based access methods.

## Conclusion
The columnar DBMS implementation provides a robust framework for managing and querying columnar data efficiently. The introduction of bitmap indexing and optimized query access methods significantly improves query performance for column-based operations. However, there is room for further optimization, particularly in batch insertion and bitmap index creation.

## References
**Database Management Systems, R. Ramakrishnan and J. Gehrke**

**Minibase**: A self-configuring database system

## License
This project is licensed under the MIT License. See the LICENSE file for details.
