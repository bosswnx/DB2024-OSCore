cd src/parser
rm -f lex.yy.cpp lex.yy.hpp yacc.tab.cpp yacc.tab.hpp yacc.tab.h
flex --header-file=lex.yy.hpp -o lex.yy.cpp lex.l
bison --defines=yacc.tab.hpp -o yacc.tab.cpp yacc.y
cd ../..