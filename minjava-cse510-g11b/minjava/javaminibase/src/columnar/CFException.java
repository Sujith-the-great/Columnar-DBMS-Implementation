/*
 * File - CFException.java
 *
 * Original Author - Vivian Roshan Adithan
 *
 * Description - 
 *		...
 */
package columnar;

import chainexception.*;

public class CFException extends ChainException {

  public CFException() {
    super();
  }

  public CFException(Exception ex, String name) {
    super(ex, name);
  }

}
