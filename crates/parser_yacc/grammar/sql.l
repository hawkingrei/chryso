/*
 * Minimal lex scaffold for chryso yacc grammar.
 */

%option noyywrap

%%
[ \t\n\r]+           ;
"select"|"SELECT"       return SELECT;
"from"|"FROM"           return FROM;
"where"|"WHERE"         return WHERE;
"distinct"|"DISTINCT"   return DISTINCT;
"on"|"ON"               return ON;
"and"|"AND"             return AND;
"or"|"OR"               return OR;
"not"|"NOT"             return NOT;
"with"|"WITH"           return WITH;
"recursive"|"RECURSIVE" return RECURSIVE;
"union"|"UNION"         return UNION;
"all"|"ALL"             return ALL;
"intersect"|"INTERSECT" return INTERSECT;
"except"|"EXCEPT"       return EXCEPT;
"insert"|"INSERT"       return INSERT;
"into"|"INTO"           return INTO;
"values"|"VALUES"       return VALUES;
"default"|"DEFAULT"     return DEFAULT;
"create"|"CREATE"       return CREATE;
"drop"|"DROP"           return DROP;
"truncate"|"TRUNCATE"   return TRUNCATE;
"if"|"IF"               return IF;
"exists"|"EXISTS"       return EXISTS;
"table"|"TABLE"         return TABLE;
"update"|"UPDATE"       return UPDATE;
"set"|"SET"             return SET;
"delete"|"DELETE"       return DELETE;
"returning"|"RETURNING" return RETURNING;
"join"|"JOIN"           return JOIN;
"left"|"LEFT"           return LEFT;
"right"|"RIGHT"         return RIGHT;
"full"|"FULL"           return FULL;
"cross"|"CROSS"         return CROSS;
"natural"|"NATURAL"     return NATURAL;
"using"|"USING"         return USING;
"as"|"AS"               return AS;
","                   return COMMA;
"."                   return DOT;
"*"                   return STAR;
"("                   return LPAREN;
")"                   return RPAREN;
"="                   return EQ;
[0-9]+(\.[0-9]+)?     return NUMBER;
"'"([^\\']|\\.|'')*"'"         return STRING;
[a-zA-Z_][a-zA-Z0-9_]* return IDENT;
[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]  return ERROR;

%%
