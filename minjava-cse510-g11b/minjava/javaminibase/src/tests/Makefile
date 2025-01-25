# modify the paths appropriately
JDKPATH = C:/Program Files/Eclipse Adoptium/jdk-21.0.2.13-hotspot/
LIBPATH = $(JDKPATH)/lib/;.;..;$(LIBPATH)
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager

BMTest:BMTest.java
	$(JAVAC) TestDriver.java BMTest.java

bmtest: BMTest
	$(JAVA) tests.BMTest

# test disk manager

DBTest:DBTest.java
	$(JAVAC) TestDriver.java DBTest.java

dbtest: DBTest
	$(JAVA) tests.DBTest

# test heapfile 

HFTest:HFTest.java
	$(JAVAC) TestDriver.java HFTest.java

hftest: HFTest
	$(JAVA) tests.HFTest

# test btree

BTTest:BTTest.java
	$(JAVAC) TestDriver.java BTTest.java

bttest: BTTest
	$(JAVA) tests.BTTest

# test index

IndexTest:IndexTest.java
	$(JAVAC) TestDriver.java IndexTest.java

indextest: IndexTest
	$(JAVA) tests.IndexTest


# test join

JoinTest:JoinTest.java
	$(JAVAC) TestDriver.java JoinTest.java

jointest: JoinTest
	$(JAVA) tests.JoinTest

# test sort

SortTest:SortTest.java
	$(JAVAC) TestDriver.java SortTest.java

sorttest: SortTest
	$(JAVA) tests.SortTest

# test sortMerge

SMJoin:SM_JoinTest.java
	$(JAVAC) SM_JoinTest.java TestDriver.java

sortmerge: SMJoin
	$(JAVA) tests.SM_JoinTest

Columnar:CFTest.java
	$(JAVAC) TestDriver.java CFTest.java

columnar:Columnar
	$(JAVA) tests.CFTest

BatchInsert:BatchInsert.java
	$(JAVAC) BatchInsert.java TestDriver.java

batchinsert: BatchInsert
	$(JAVA) tests.BatchInsert $(DATAFILENAME) $(COLUMNDBNAME) $(COLUMNARFILENAME) $(NUMCOLUMNS)

Query:QueryProgram.java
	$(JAVAC) QueryProgram.java

query:Query
	$(JAVA) tests.QueryProgram $(COLUMNDBNAME) $(COLUMNARFILENAME) $(TARGETCOLUMNNAMES) $(VALUECONSTRAINT) $(NUMBUF) $(ACCESSTYPE)

QueryDelete:QueryDeleteProgram.java
	$(JAVAC) QueryDeleteProgram.java

query_delete:QueryDelete
	$(JAVA) tests.QueryDeleteProgram $(COLUMNDBNAME) $(COLUMNARFILENAME) $(TARGETCOLUMNNAMES) $(VALUECONSTRAINT) $(NUMBUF) $(ACCESSTYPE) $(PURGEDB)

Index: Index.java
	$(JAVAC) Index.java TestDriver.java

index: Index
	$(JAVA) tests.Index $(COLUMNDBNAME) $(COLUMNARFILENAME) $(COLUMNNAME) $(INDEXTYPE)
