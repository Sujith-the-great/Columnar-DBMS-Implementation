package global;

public class IntegerValueClass extends ValueClass
{
	public int valueType = AttrType.attrInteger;
	public int classValue = -1;
	public int valueLength = 1; //shouldn't be used in this implementation
	
	public IntegerValueClass() {}
	
	public IntegerValueClass( int value )
	{
		classValue = value;
	}
	
	public byte[] getClassValue()
		throws java.io.IOException
	{
		byte[] data = new byte[GlobalConst.MAX_NAME];
		Convert.setIntValue(classValue, 0, data);
		return data;
	}
}
