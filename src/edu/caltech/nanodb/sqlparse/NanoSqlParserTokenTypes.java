// $ANTLR 2.7.7 (20060906): "nanosql.g" -> "NanoSqlLexer.java"$

  /**
   * Copyright (c) 2005-2011 by the California Institute of Technology.
   * All rights reserved.
   */
  package edu.caltech.nanodb.sqlparse;

  import java.util.ArrayList;
  import java.util.List;

  import edu.caltech.nanodb.commands.*;
  import edu.caltech.nanodb.expressions.*;
  import edu.caltech.nanodb.relations.*;

public interface NanoSqlParserTokenTypes {
	int EOF = 1;
	int NULL_TREE_LOOKAHEAD = 3;
	int ADD = 4;
	int ALL = 5;
	int ALTER = 6;
	int ANALYZE = 7;
	int AND = 8;
	int ANY = 9;
	int AS = 10;
	int ASC = 11;
	int BEGIN = 12;
	int BETWEEN = 13;
	int BY = 14;
	int CASCADE = 15;
	int COLUMN = 16;
	int COMMIT = 17;
	int CONSTRAINT = 18;
	int CRASH = 19;
	int CREATE = 20;
	int CROSS = 21;
	int DEFAULT = 22;
	int DELETE = 23;
	int DESC = 24;
	int DISTINCT = 25;
	int DROP = 26;
	int DUMP = 27;
	int EXCEPT = 28;
	int EXISTS = 29;
	int EXIT = 30;
	int EXPLAIN = 31;
	int FALSE = 32;
	int FILE = 33;
	int FLUSH = 34;
	int FOREIGN = 35;
	int FORMAT = 36;
	int FROM = 37;
	int FULL = 38;
	int GROUP = 39;
	int HAVING = 40;
	int IF = 41;
	int IN = 42;
	int INDEX = 43;
	int INNER = 44;
	int INSERT = 45;
	int INTERSECT = 46;
	int INTO = 47;
	int IS = 48;
	int JOIN = 49;
	int KEY = 50;
	int LEFT = 51;
	int LIKE = 52;
	int LIMIT = 53;
	int MINUS = 54;
	int NATURAL = 55;
	int NOT = 56;
	int NULL = 57;
	int OFFSET = 58;
	int ON = 59;
	int OPTIMIZE = 60;
	int OR = 61;
	int ORDER = 62;
	int OUTER = 63;
	int PRIMARY = 64;
	int PROPERTIES = 65;
	int QUIT = 66;
	int REFERENCES = 67;
	int RENAME = 68;
	int RESTRICT = 69;
	int RIGHT = 70;
	int ROLLBACK = 71;
	int SELECT = 72;
	int SET = 73;
	int SHOW = 74;
	int SIMILAR = 75;
	int SOME = 76;
	int START = 77;
	int TABLE = 78;
	int TO = 79;
	int TRANSACTION = 80;
	int TRUE = 81;
	int TYPE = 82;
	int UNION = 83;
	int UNIQUE = 84;
	int UNKNOWN = 85;
	int UPDATE = 86;
	int USING = 87;
	int VALUES = 88;
	int VARIABLE = 89;
	int VARIABLES = 90;
	int VERBOSE = 91;
	int VERIFY = 92;
	int VIEW = 93;
	int WHERE = 94;
	int WITH = 95;
	int WORK = 96;
	int TYPE_BIGINT = 97;
	int TYPE_BLOB = 98;
	int TYPE_CHAR = 99;
	int TYPE_CHARACTER = 100;
	int TYPE_DATE = 101;
	int TYPE_DATETIME = 102;
	int TYPE_DECIMAL = 103;
	int TYPE_FLOAT = 104;
	int TYPE_DOUBLE = 105;
	int TYPE_INT = 106;
	int TYPE_INTEGER = 107;
	int TYPE_NUMERIC = 108;
	int TYPE_TEXT = 109;
	int TYPE_TIME = 110;
	int TYPE_TIMESTAMP = 111;
	int TYPE_VARCHAR = 112;
	int TYPE_VARYING = 113;
	int INT_LITERAL = 114;
	int LONG_LITERAL = 115;
	int FLOAT_LITERAL = 116;
	int DEC_LITERAL = 117;
	int PERIOD = 118;
	int SEMICOLON = 119;
	int IDENT = 120;
	int QUOTED_IDENT = 121;
	int TEMPORARY = 122;
	int LPAREN = 123;
	int COMMA = 124;
	int RPAREN = 125;
	int EQUALS = 126;
	int STAR = 127;
	int STRING_LITERAL = 128;
	int NOT_EQUALS = 129;
	int GRTR_THAN = 130;
	int LESS_THAN = 131;
	int GRTR_EQUAL = 132;
	int LESS_EQUAL = 133;
	int PLUS = 134;
	int SLASH = 135;
	int PERCENT = 136;
	int COLON = 137;
	int NEWLINE = 138;
	int WS = 139;
	int COMMENT = 140;
	int COMPARE_OPERATOR = 141;
	int NUM_LITERAL_OR_SYMBOL = 142;
}
