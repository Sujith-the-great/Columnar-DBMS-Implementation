JDKPATH = C:/Program Files/Eclipse Adoptium/jdk-21.0.2.13-hotspot/
LIBPATH = $(JDKPATH)/lib/;.;..;$(LIBPATH)
CLASSPATH = $(JDKPATH)/lib/classes.zip;.;..;$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

bm:BM.java
	$(JAVAC) BM.java

bmf:BitMapFile.java
	$(JAVAC) BitMapFile.java

bmp:BMPage.java
	$(JAVAC) BMPage.java
	
bmfs:BMFileScan.java
	$(JAVAC) BMFileScan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
