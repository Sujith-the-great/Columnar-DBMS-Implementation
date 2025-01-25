
package global;

import java.io.IOException;

public class FloatValueClass extends ValueClass
{
	public int valueType = AttrType.attrInteger;
	public float classValue = -1;
	public int valueLength = 1; //shouldn't be used in this implementation
	
	public FloatValueClass() {}
	
	public FloatValueClass( float value )
	{
		classValue = value;
	}
	
	public byte[] getClassValue()
		throws IOException
	{
		byte[] data = new byte[GlobalConst.MAX_NAME];
		Convert.setFloValue(classValue, 0, data);
		return data;
	}
}
