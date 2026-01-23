%start Statement

%token SELECT FROM WHERE DISTINCT ON AND OR NOT
%token WITH RECURSIVE UNION ALL INTERSECT EXCEPT
%token INSERT INTO VALUES DEFAULT UPDATE SET DELETE RETURNING
%token CREATE DROP TRUNCATE IF EXISTS TABLE
%token JOIN LEFT RIGHT FULL CROSS NATURAL USING AS
%token COMMA DOT STAR LPAREN RPAREN EQ
%token IDENT NUMBER STRING

%%

Statement
  : WithStmt
  | SelectStmt
  | InsertStmt
  | UpdateStmt
  | DeleteStmt
  | CreateStmt
  | DropStmt
  | TruncateStmt
  ;

CreateStmt
  : CREATE TABLE IfNotExistsOpt IDENT ColumnDefsOpt
  ;

DropStmt
  : DROP TABLE IfExistsOpt IDENT
  ;

IfExistsOpt
  :
  | IF EXISTS
  ;

IfNotExistsOpt
  :
  | IF NOT EXISTS
  ;

TruncateStmt
  : TRUNCATE TableOpt IDENT
  ;

TableOpt
  :
  | TABLE
  ;

ColumnDefsOpt
  :
  | LPAREN ColumnDefList RPAREN
  ;

ColumnDefList
  : ColumnDef
  | ColumnDefList COMMA ColumnDef
  ;

ColumnDef
  : IDENT TypeName
  ;

TypeName
  : IDENT
  | IDENT IDENT
  | IDENT LPAREN NUMBER RPAREN
  | IDENT IDENT LPAREN NUMBER RPAREN
  ;

WithStmt
  : WITH RecursiveOpt CteList Statement
  ;

RecursiveOpt
  :
  | RECURSIVE
  ;

CteList
  : Cte
  | CteList COMMA Cte
  ;

Cte
  : IDENT OptColumnList AS LPAREN Statement RPAREN
  ;

OptColumnList
  :
  | LPAREN IdentList RPAREN
  ;

SelectStmt
  : SelectCore
  | SelectStmt UNION SelectCore
  | SelectStmt UNION ALL SelectCore
  | SelectStmt INTERSECT SelectCore
  | SelectStmt INTERSECT ALL SelectCore
  | SelectStmt EXCEPT SelectCore
  | SelectStmt EXCEPT ALL SelectCore
  ;

SelectCore
  : SELECT DistinctOpt SelectList FromClauseOpt WhereClause
  ;

FromClauseOpt
  :
  | FromClause
  ;

DistinctOpt
  :
  | DISTINCT
  | DISTINCT ON LPAREN ExprList RPAREN
  ;

SelectList
  : STAR
  | SelectList COMMA Expr
  | Expr
  ;

FromClause
  : FROM TableRefList
  ;

TableRefList
  : TableRef
  | TableRefList COMMA TableRef
  ;

TableRef
  : TableFactor OptAlias JoinList
  ;

TableFactor
  : IDENT
  | LPAREN SelectStmt RPAREN
  | LPAREN WithStmt RPAREN
  ;

OptAlias
  :
  | AS IDENT ColumnAliasListOpt
  | IDENT ColumnAliasListOpt
  ;

ColumnAliasListOpt
  :
  | LPAREN IdentList RPAREN
  ;

JoinList
  :
  | JoinList JoinClause
  ;

JoinClause
  : RegularJoin
  | CrossJoin
  | NaturalJoin
  ;

RegularJoin
  : JoinTypeRegular TableRef JoinCondition
  ;

CrossJoin
  : CROSS JOIN TableRef
  ;

NaturalJoin
  : NaturalJoinType TableRef
  ;

JoinTypeRegular
  : JOIN
  | LEFT JOIN
  | RIGHT JOIN
  | FULL JOIN
  ;

NaturalJoinType
  : NATURAL JOIN
  | NATURAL LEFT JOIN
  | NATURAL RIGHT JOIN
  | NATURAL FULL JOIN
  ;

JoinCondition
  : ON Expr
  | USING LPAREN IdentList RPAREN
  ;

WhereClause
  :
  | WHERE Expr
  ;

InsertStmt
  : INSERT INTO IDENT OptColumnList DEFAULT VALUES ReturningClause
  | INSERT INTO IDENT OptColumnList VALUES ValuesList ReturningClause
  | INSERT INTO IDENT OptColumnList SelectStmt ReturningClause
  ;

ValuesList
  : LPAREN ExprList RPAREN
  | ValuesList COMMA LPAREN ExprList RPAREN
  ;

UpdateStmt
  : UPDATE IDENT SET AssignmentList WhereClause ReturningClause
  ;

DeleteStmt
  : DELETE FROM IDENT WhereClause ReturningClause
  ;

ReturningClause
  :
  | RETURNING SelectList
  ;

AssignmentList
  : Assignment
  | AssignmentList COMMA Assignment
  ;

Assignment
  : IDENT EQ Expr
  ;

ExprList
  : Expr
  | ExprList COMMA Expr
  ;

IdentList
  : IDENT
  | IdentList COMMA IDENT
  ;

Expr
  : Expr AND Expr
  | Expr OR Expr
  | Expr EQ Expr
  | LPAREN Expr RPAREN
  | IDENT DOT IDENT
  | IDENT DOT STAR
  | IDENT
  | NUMBER
  | STRING
  ;

%%
