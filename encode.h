#ifndef ENCODE_H_
#define ENCODE_H_
void nysiis(const char *, char **, int);
void doubleMetaphone(const char *, char **, int );
void soundex(char *text, char *buffer, size_t len);
void brsoundex(char *text, char *buffer, size_t len);
#endif /*ENCODE_H_*/
