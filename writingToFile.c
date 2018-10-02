{\rtf1\ansi\ansicpg1252\cocoartf1671
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 \
int BiggestNumByte = 10;\
\
//writes each pair array for each reduce thread
void writePairsToFile(char* filename, int numOfReduces, void* shareMem)\{\
    \
    FILE *fp;\
    int i;\
    int j;\
    fp = fopen( filename, "w" );\
\
    if (fp == NULL)\
    \{\
        printf("Error opening file!\\n");\
        exit(1);\
    \}\
\
    for(i = 0; i < numOfReduces; i++)\{\
\
    struct pair* pairArr = getCopyOfPairArr( shareMem, i);\
     \
    j = 0;\
    while(1)\{\
       \
       if(pairArr[j].value == -1)\{break;\}\
        \
      char* snum = (char*)malloc( BiggestNumByte * sizeof(char));\
      sprintf(snum, "%d", pairArr[j].value);    \
      fprintf(fp, pairArr[j].key );\
      fprintf(fp, " ");\
      fprintf(fp, snum );\
      \
      \
\
      fprintf(fp,"\\n");\
      free(snum);\
       j++;\
     \
        \}\
     free(pairArr);\
    \} \
   \
    \
    fclose(fp);\
\}}
