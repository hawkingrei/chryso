%start Statement

%token SELECT FROM WHERE DISTINCT ON AND OR NOT
%token WITH RECURSIVE
%token INSERT INTO VALUES DEFAULT UPDATE SET DELETE RETURNING
%token CREATE DROP TRUNCATE IF EXISTS TABLE
%token JOIN LEFT RIGHT FULL CROSS NATURAL USING AS
%token GROUP BY HAVING ORDER LIMIT OFFSET
%token ASC DESC NULLS FIRST LAST
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
  : ColumnDef ColumnDefListTail
  ;

ColumnDefListTail
  :
  | COMMA ColumnDef ColumnDefListTail
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
  : Cte CteListTail
  ;

CteListTail
  :
  | COMMA Cte CteListTail
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
  ;

SelectCore
  : SELECT DistinctOpt SelectList FromClauseOpt WhereClause GroupByClauseOpt HavingClauseOpt OrderByClauseOpt LimitClauseOpt OffsetClauseOpt
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
  | Expr SelectListTail
  ;

SelectListTail
  :
  | COMMA Expr SelectListTail
  ;

FromClause
  : FROM TableRefList
  ;

TableRefList
  : TableRef TableRefListTail
  ;

TableRefListTail
  :
  | COMMA TableRef TableRefListTail
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
  | JoinClause JoinList
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

GroupByClauseOpt
  :
  | GROUP BY ExprList
  ;

HavingClauseOpt
  :
  | HAVING Expr
  ;

OrderByClauseOpt
  :
  | ORDER BY OrderByList
  ;

OrderByList
  : OrderByExpr OrderByListTail
  ;

OrderByListTail
  :
  | COMMA OrderByExpr OrderByListTail
  ;

OrderByExpr
  : Expr OptSortDirection OptNullsOrder
  ;

OptSortDirection
  :
  | ASC
  | DESC
  ;

OptNullsOrder
  :
  | NULLS FIRST
  | NULLS LAST
  ;

LimitClauseOpt
  :
  | LIMIT NUMBER
  ;

OffsetClauseOpt
  :
  | OFFSET NUMBER
  ;

InsertStmt
  : INSERT INTO IDENT OptColumnList DEFAULT VALUES ReturningClause
  | INSERT INTO IDENT OptColumnList VALUES ValuesList ReturningClause
  | INSERT INTO IDENT OptColumnList SelectStmt ReturningClause
  ;

ValuesList
  : ValuesRow ValuesListTail
  ;

ValuesListTail
  :
  | COMMA ValuesRow ValuesListTail
  ;

ValuesRow
  : LPAREN ExprList RPAREN
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
  : Assignment AssignmentListTail
  ;

AssignmentListTail
  :
  | COMMA Assignment AssignmentListTail
  ;

Assignment
  : IDENT EQ Expr
  ;

ExprList
  : Expr ExprListTail
  ;

ExprListTail
  :
  | COMMA Expr ExprListTail
  ;

IdentList
  : IDENT IdentListTail
  ;

IdentListTail
  :
  | COMMA IDENT IdentListTail
  ;

Expr
  : OrExpr
  ;

OrExpr
  : AndExpr OrExprTail
  ;

OrExprTail
  :
  | OR AndExpr OrExprTail
  ;

AndExpr
  : EqExpr AndExprTail
  ;

AndExprTail
  :
  | AND EqExpr AndExprTail
  ;

EqExpr
  : PrimaryExpr EqExprTail
  ;

EqExprTail
  :
  | EQ PrimaryExpr EqExprTail
  ;

PrimaryExpr
  : LPAREN Expr RPAREN
  | IDENT PrimarySuffixOpt
  | NUMBER
  | STRING
  ;

PrimarySuffixOpt
  :
  | DOT IDENT
  | DOT STAR
  ;

%%
