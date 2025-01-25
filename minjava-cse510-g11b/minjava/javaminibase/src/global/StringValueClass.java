
package global;
import java.lang.*;

public class StringValueClass extends ValueClass
{
	public int valueType = AttrType.attrString;
	public String classValue = "";
	public int valueLength = 0;
	
	public StringValueClass() {}
	
	public StringValueClass( String value )
	{
		classValue = value;
		valueLength = classValue.length();
	}
	
	public byte[] getClassValue()
		throws java.io.IOException
	{
		byte[] data = new byte[GlobalConst.MAX_NAME];
		Convert.setStrValue(classValue, 0, data);
		return data;
	}
}
