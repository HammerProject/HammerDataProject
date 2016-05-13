/* Generated By:JavaCC: Do not edit this line. Isabella.java */
package org.hammer.isabella.cc;


import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.InstanceNode;
import org.hammer.isabella.query.Edge;
import org.hammer.isabella.query.QuestionEdge;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.cc.util.IsabellaUtils;
import org.hammer.isabella.cc.util.SemanticUtil;
import org.hammer.isabella.query.AttributeValue;

/**
* Isabella main class 
*/
public class Isabella implements IsabellaConstants {

        private SortedMap<Integer, IsabellaError> errors = new TreeMap<Integer, IsabellaError> ();
        private QueryGraph qG = null;

        public SortedMap<Integer, IsabellaError> getErrors() {
                return errors;
        }

/********************************************************************************
*  QUERY GRAPH
*/
  final public QueryGraph queryGraph() throws ParseException {
    try {
      jj_consume_token(LB);
      qG = queryGraphAttr();
      jj_consume_token(RB);
      jj_consume_token(0);
    } catch (ParseException ex) {
                IsabellaUtils.Recover(this, errors, ex, Isabella.RB);
    }
         {if (true) return qG;}
    throw new Error("Missing return statement in function");
  }

/**
* Query Graph attr
*/
  final public QueryGraph queryGraphAttr() throws ParseException {
        ArrayList<QuestionEdge> qList = new ArrayList<QuestionEdge>();
        ArrayList<InstanceNode> iList = new ArrayList<InstanceNode>();
        ArrayList<Edge> wList = new ArrayList<Edge>();
    try {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case SELECT:
        qList = select();
        break;
      case FROM:
        iList = from();
        break;
      case WHERE:
        wList = where();
        break;
      default:
        jj_consume_token(-1);
        throw new ParseException();
      }
      label_1:
      while (true) {
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case COMMA:
          ;
          break;
        default:
          break label_1;
        }
        jj_consume_token(COMMA);
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case SELECT:
          qList = select();
          break;
        case FROM:
          iList = from();
          break;
        case WHERE:
          wList = where();
          break;
        default:
          jj_consume_token(-1);
          throw new ParseException();
        }
      }
    } catch (ParseException ex) {
                IsabellaUtils.Recover(this, errors, ex, Isabella.RS);
    }
                {if (true) return SemanticUtil.Build(errors, qList, iList, wList);}
    throw new Error("Missing return statement in function");
  }

/**
* Select
*/
  final public ArrayList<QuestionEdge> select() throws ParseException {
        ArrayList<QuestionEdge> qList = new ArrayList<QuestionEdge>();
        QuestionEdge eN = null;
    try {
      jj_consume_token(SELECT);
      jj_consume_token(COLON);
      jj_consume_token(LS);
      jj_consume_token(LB);
      eN = selectAttr();
                                        qList.add(eN);
      jj_consume_token(RB);
      label_2:
      while (true) {
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case COMMA:
          ;
          break;
        default:
          break label_2;
        }
        jj_consume_token(COMMA);
        jj_consume_token(LB);
        eN = selectAttr();
                                        qList.add(eN);
        jj_consume_token(RB);
      }
      jj_consume_token(RS);
    } catch (ParseException ex) {
                IsabellaUtils.Recover(this, errors, ex, Isabella.RS);
    }
                {if (true) return qList;}
    throw new Error("Missing return statement in function");
  }

/**
* From attr
*/
  final public ArrayList<InstanceNode> from() throws ParseException {
        ArrayList<InstanceNode> iList = new ArrayList<InstanceNode>();
        InstanceNode iN = null;
    try {
      jj_consume_token(FROM);
      jj_consume_token(COLON);
      jj_consume_token(LS);
      iN = fromAttr();
                          iList.add(iN);
      label_3:
      while (true) {
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case COMMA:
          ;
          break;
        default:
          break label_3;
        }
        jj_consume_token(COMMA);
        iN = fromAttr();
                                 iList.add(iN);
      }
      jj_consume_token(RS);
    } catch (ParseException ex) {
                IsabellaUtils.Recover(this, errors, ex, Isabella.RS);
    }
                {if (true) return iList;}
    throw new Error("Missing return statement in function");
  }

/**
* Where attr
*/
  final public ArrayList<Edge> where() throws ParseException {
        ArrayList<Edge> wList = new ArrayList<Edge>();
        Edge wN = null;
    try {
      jj_consume_token(WHERE);
      jj_consume_token(COLON);
      jj_consume_token(LS);
      label_4:
      while (true) {
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case LB:
          ;
          break;
        default:
          break label_4;
        }
        jj_consume_token(LB);
        wN = whereAttr();
                                 if(wN != null) wList.add(wN);
        jj_consume_token(RB);
        label_5:
        while (true) {
          switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
          case COMMA:
            ;
            break;
          default:
            break label_5;
          }
          jj_consume_token(COMMA);
          jj_consume_token(LB);
          wN = whereAttr();
                                         if(wN != null) wList.add(wN);
          jj_consume_token(RB);
        }
      }
      jj_consume_token(RS);
    } catch (ParseException ex) {
                IsabellaUtils.Recover(this, errors, ex, Isabella.RS);
    }
                {if (true) return wList;}
    throw new Error("Missing return statement in function");
  }

/********************************************************************************
* Select Attr
*
*/
  final public QuestionEdge selectAttr() throws ParseException {
    SortedMap<String, AttributeValue> sl = new TreeMap<String, AttributeValue>();
    try {
      jj_consume_token(COLUMN);
      jj_consume_token(COLON);
      jj_consume_token(LB);
      sl = listAttr();
      jj_consume_token(RB);
    } catch (ParseException ex) {
                IsabellaUtils.Recover(this, errors, ex, Isabella.RB);
    }
         {if (true) return SemanticUtil.SetAttrValueSelect(sl, errors, this.token);}
    throw new Error("Missing return statement in function");
  }

/********************************************************************************
* From Attr
*
*/
  final public InstanceNode fromAttr() throws ParseException {
    try {
      jj_consume_token(LITERAL);
    } catch (ParseException ex) {
                {if (true) throw ex;}
    }
         {if (true) return SemanticUtil.SetAttrValueFrom(errors, this.token);}
    throw new Error("Missing return statement in function");
  }

/********************************************************************************
* Where Attr
*
*/
  final public Edge whereAttr() throws ParseException {
    SortedMap<String, AttributeValue> sl = new TreeMap<String, AttributeValue>();
    try {
      jj_consume_token(CONDITION);
      jj_consume_token(COLON);
      jj_consume_token(LB);
      sl = listAttr();
      jj_consume_token(RB);
    } catch (ParseException ex) {
                IsabellaUtils.Recover(this, errors, ex, Isabella.RB);
    }
         {if (true) return SemanticUtil.SetAttrValueWhere(sl, errors, this.token);}
    throw new Error("Missing return statement in function");
  }

/********************************************************************************
* Lista Attr
*
*/
  final public SortedMap<String, AttributeValue> listAttr() throws ParseException {
    SortedMap<String, AttributeValue> sl = new TreeMap<String, AttributeValue>();
    AttributeValue av;
    try {
      av = attrValue();
                                if (av.getName() != null) sl.put(av.getName(), av);
      label_6:
      while (true) {
        switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
        case COMMA:
          ;
          break;
        default:
          break label_6;
        }
        jj_consume_token(COMMA);
        av = attrValue();
                                            if (av.getName() != null) sl.put(av.getName(), av);
      }
    } catch (ParseException ex) {
                IsabellaUtils.Recover(this, errors, ex, Isabella.LITERAL);
    }
         {if (true) return sl;}
    throw new Error("Missing return statement in function");
  }

/**
* Attr value
*/
  final public AttributeValue attrValue() throws ParseException {
        Token t = null;
        AttributeValue av = new AttributeValue();
    try {
      t = jj_consume_token(LITERAL);
      jj_consume_token(COLON);
                                        av.setName(t.image.substring(1,t.image.length()-1));
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case LITERAL:
        t = jj_consume_token(LITERAL);
                                        av.setLiteralValue(t.image.substring(1,t.image.length()-1).replaceAll("\u005c\u005c\u005c"","\u005c""));
        break;
      case NUMBER:
        t = jj_consume_token(NUMBER);
                                    av.setNumberValue(Integer.parseInt(t.image));
        break;
      case FLOATING_NUMBER:
        t = jj_consume_token(FLOATING_NUMBER);
                                            av.setFloatValue(Float.parseFloat(t.image));
        break;
      default:
        jj_consume_token(-1);
        throw new ParseException();
      }
    } catch (ParseException ex) {
        IsabellaUtils.Recover(this, errors, ex, Isabella.LITERAL);
    }
    av.eval(errors, this.token.beginLine, this.token.beginColumn);
        {if (true) return av;}
    throw new Error("Missing return statement in function");
  }

  /** Generated Token Manager. */
  public IsabellaTokenManager token_source;
  SimpleCharStream jj_input_stream;
  /** Current token. */
  public Token token;
  /** Next token. */
  public Token jj_nt;
  private int jj_ntk;

  /** Constructor with InputStream. */
  public Isabella(java.io.InputStream stream) {
     this(stream, null);
  }
  /** Constructor with InputStream and supplied encoding */
  public Isabella(java.io.InputStream stream, String encoding) {
    try { jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source = new IsabellaTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
  }

  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
  }

  /** Constructor. */
  public Isabella(java.io.Reader stream) {
    jj_input_stream = new SimpleCharStream(stream, 1, 1);
    token_source = new IsabellaTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
  }

  /** Reinitialise. */
  public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
  }

  /** Constructor with generated Token Manager. */
  public Isabella(IsabellaTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
  }

  /** Reinitialise. */
  public void ReInit(IsabellaTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
  }

  private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      return token;
    }
    token = oldToken;
    throw generateParseException();
  }


/** Get the next Token. */
  final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    return token;
  }

/** Get the specific Token. */
  final public Token getToken(int index) {
    Token t = token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  private int jj_ntk() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  /** Generate ParseException. */
  public ParseException generateParseException() {
    Token errortok = token.next;
    int line = errortok.beginLine, column = errortok.beginColumn;
    String mess = (errortok.kind == 0) ? tokenImage[0] : errortok.image;
    return new ParseException("Parse error at line " + line + ", column " + column + ".  Encountered: " + mess);
  }

  /** Enable tracing. */
  final public void enable_tracing() {
  }

  /** Disable tracing. */
  final public void disable_tracing() {
  }

}
