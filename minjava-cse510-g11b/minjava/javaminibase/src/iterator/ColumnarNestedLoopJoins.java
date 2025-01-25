package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import columnar.Columnarfile;
import columnar.TupleScan;
import diskmgr.*;


import java.lang.*;
import java.io.*;
import java.util.*;

public class ColumnarNestedLoopJoins extends Iterator {
    private AttrType[] r;
    private int len_r;
    private AttrType[] c;
    private int len_c;
    private short[] c_str_sizes;
    private Iterator itr;
    private String columnarFileName;
    private CondExpr[] OutputFilter;
    private CondExpr[] RightFilter;
    private FldSpec[] proj;
    private int buffer;
    private boolean completed,outer;
    private Heapfile hf;
    private Scan sc;
    private Tuple outer_tuple, inner_tuple;
    private Tuple joined_tuple; 


    public ColumnarNestedLoopJoins(
            AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            AttrType[] in2,
            int len_in2,
            short[] t2_str_sizes,
            int amt_of_mem,
            Iterator am1,
            String columnarFileName,
            CondExpr[] outFilter,
            CondExpr[] rightFilter,
            FldSpec[] proj_list,
            int n_out_flds) {  
      
      AttrType[] r=new AttrType[in1.length];
      AttrType[] s=new AttrType[in2.length];
      r=Arrays.copyOf(in1,in1.length);
      c=Arrays.copyOf(in2,in2.length);
      int in1_len=len_in1;
      int in2_len=len_in2;
             
      itr = am1;
      c_str_sizes=t2_str_sizes;
      inner_tuple = new Tuple();
      outer_tuple = new Tuple();
      joined_tuple = new Tuple();
      OutputFilter = outFilter;
      RightFilter  = rightFilter;
      
      buffer    = amt_of_mem;
      sc = null;
      boolean done  = false;
      outer = true;
      
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    t_size;
      
      FldSpec[] list_proj = proj_list;
      int OutFlds = n_out_flds;


      try {
	           t_size = TupleUtils.setup_op_tuple(joined_tuple, Jtypes,
					   in1, len_in1, in2, len_in2,
					   t1_str_sizes, t2_str_sizes,
					   proj_list, OutFlds);
      }catch (TupleUtilsException e){
	throw new NestedLoopException(e,"TupleUtilsException");
      }
      
      
      
      try {
	  hf = new Heapfile(columnarFileName);
	  
      }
      catch(Exception e) {
	throw new NestedLoopException(e, "heapfile failed.");
      }
    }
  
  public Tuple get_next()
    throws Exception
    {
      
      
      if (done)
	      return null;
      
      do
	{
	  
	  if (outer == true)
	    {
	      outer = false;
	   	    
	      try {
		sc = hf.openScan();
	      }
	      catch(Exception e){
		throw new NestedLoopException(e, "openScan failed");
	      }
	      
	      if ((outer_tuple=outer.get_next()) == null)
		{
		  done = true;
		  if (sc != null) 
		    {
                      
		      sc = null;
		    }
		  
		  return null;
		}   
	    } 
	 
	      RID rid = new RID();
	      while ((inner_tuple = sc.getNext(rid)) != null)
		{
		  inner_tuple.setHdr((short)in2_len, c,c_str_sizes);
		  if (PredEval.Eval(RightFilter, inner_tuple, null, c, null) == true)
		    {
		      if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, r, c) == true)
			{
			  Projection.Join(outer_tuple, r, 
					  inner_tuple, c, 
					  Jtuple, list_proj, OutFlds);
			  return Jtuple;
			}
		    }
		}
	      
	      outer = true;       
	} while (true);
    } 
 
  public void close() throws JoinsException, IOException,IndexException 
    {
      if (!closeFlag) {
	
	try {
	  outer.close();
	}
  catch (Exception e) {
	  throw new Exception(e);
	}
	closeFlag = true;
      }
    }
