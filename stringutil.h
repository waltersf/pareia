#include <cstdlib>

#ifndef __STRINGUTIL_H__
#define __STRINGUTIL_H__

#include <string>

#define MAX(a, b) (a>b?a:b)
#define MIN(a, b) (a<b?a:b)
#define MIN3(a, b, c) (a < b? (a < c?a:c):(b < c?b:c))

char *trim(char *str);
float jaro(const char *s, const char *t);
float winkler(const char *s, const char *t);
float editdistance_score(const char *s, const char *t);
float exact(char *s, char *t);
int bmh(char *s, char *t);
void nysiis(char *s, char *buffer, int maxlen);
int bmhAfter(char *s, char *t, int start);
void replaceAll(char **t, const char *p, const char *r, int pos);
int str_is_vowel(char);
char *str_itoa(int, char *, int);
std::string str_itoa_cpp( int value, int radix );

#endif
