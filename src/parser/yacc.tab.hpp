/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_YY_YACC_TAB_HPP_INCLUDED
#define YY_YY_YACC_TAB_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
#define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
#define YYTOKENTYPE
enum yytokentype {
    SHOW = 258,
    TABLES = 259,
    CREATE = 260,
    TABLE = 261,
    DROP = 262,
    DESC = 263,
    INSERT = 264,
    INTO = 265,
    VALUES = 266,
    DELETE = 267,
    FROM = 268,
    ASC = 269,
    ORDER = 270,
    GROUP = 271,
    BY = 272,
    HAVING = 273,
    WHERE = 274,
    UPDATE = 275,
    SET = 276,
    SELECT = 277,
    MAX = 278,
    MIN = 279,
    SUM = 280,
    COUNT = 281,
    AS = 282,
    INT = 283,
    CHAR = 284,
    FLOAT = 285,
    DATE = 286,
    INDEX = 287,
    AND = 288,
    JOIN = 289,
    EXIT = 290,
    HELP = 291,
    TXN_BEGIN = 292,
    TXN_COMMIT = 293,
    TXN_ABORT = 294,
    TXN_ROLLBACK = 295,
    ORDER_BY = 296,
    ENABLE_NESTLOOP = 297,
    ENABLE_SORTMERGE = 298,
    LEQ = 299,
    NEQ = 300,
    GEQ = 301,
    T_EOF = 302,
    IDENTIFIER = 303,
    VALUE_STRING = 304,
    VALUE_INT = 305,
    VALUE_FLOAT = 306,
    VALUE_BOOL = 307,
    VALUE_DATE = 308
};
#endif

/* Value type.  */

/* Location type.  */
#if !defined YYLTYPE && !defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE {
    int first_line;
    int first_column;
    int last_line;
    int last_column;
};
#define YYLTYPE_IS_DECLARED 1
#define YYLTYPE_IS_TRIVIAL 1
#endif

int yyparse(void);

#endif /* !YY_YY_YACC_TAB_HPP_INCLUDED  */
