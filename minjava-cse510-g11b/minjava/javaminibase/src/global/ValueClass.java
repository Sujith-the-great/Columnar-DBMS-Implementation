package global;

public abstract class ValueClass extends java.lang.Object
{
	public int valueType = AttrType.attrNull;
	public int valueLength;
	
	public ValueClass() {}
	
	public abstract byte[] getClassValue() throws java.io.IOException;
	
	public int getValueLength()
	{
		return valueLength;
	}
	
	public int getType()
	{
		return valueType;
	}
	
	public void setType( int type )
	{
		valueType = type;
	}
}
