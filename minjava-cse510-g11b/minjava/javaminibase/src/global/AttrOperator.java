package global;

/** 
 * Enumeration class for AttrOperator
 * 
 */

public class AttrOperator {

  public static final int aopEQ   = 0;
  public static final int aopLT   = 1;
  public static final int aopGT   = 2;
  public static final int aopNE   = 3;
  public static final int aopLE   = 4;
  public static final int aopGE   = 5;
  public static final int aopNOT  = 6;
  public static final int aopNOP  = 7;
  public static final int opRANGE = 8; //defined this way in C++

  public int attrOperator;

  /** 
   * AttrOperator Constructor
   * <br>
   * An attribute operator types can be defined as 
   * <ul>
   * <li>   AttrOperator attrOperator = new AttrOperator(AttrOperator.aopEQ);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (attrOperator.attrOperator == AttrOperator.aopEQ) ....
   * </ul>
   *
   * @param _attrOperator The available attribute operators 
   */

  public AttrOperator (int _attrOperator) {
    attrOperator = _attrOperator;
  }

  public AttrOperator (String _attrOperator){
    if (_attrOperator.equals("aopEQ") || _attrOperator.equals("=")) {
      attrOperator = aopEQ;
    } else if (_attrOperator.equals("aopLT") || _attrOperator.equals("<")) {
      attrOperator = aopLT;
    } else if (_attrOperator.equals("aopGT") || _attrOperator.equals(">")) {
      attrOperator = aopGT;
    } else if (_attrOperator.equals("aopNE") || _attrOperator.equals("!=")) {
      attrOperator = aopNE;
    } else if (_attrOperator.equals("aopLE") || _attrOperator.equals("<=")) {
      attrOperator = aopLE;
    } else if (_attrOperator.equals("aopGE") || _attrOperator.equals(">=")) {
      attrOperator = aopGE;
    } else if (_attrOperator.equals("aopNOT") || _attrOperator.equals("NOT")) {
      attrOperator = aopNOT;
    } else if (_attrOperator.equals("aopNOP") || _attrOperator.equals("NOP")) {
      attrOperator = aopNOP;
    } else if (_attrOperator.equals("opRANGE") || _attrOperator.equals("RANGE")) {
      attrOperator = opRANGE;
    }
  }

  public String toString() {

    switch (attrOperator) {
    case aopEQ:
      return "aopEQ";
    case aopLT:
      return "aopLT";
    case aopGT:
      return "aopGT";
    case aopNE:
      return "aopNE";
    case aopLE:
      return "aopLE";
    case aopGE:
      return "aopGE";
    case aopNOT:
      return "aopNOT";
    case aopNOP:
      return "aopNOP";
    case opRANGE:
      return "opRANGE";
    }
    return ("Unexpected AttrOperator " + attrOperator);
  }
}
