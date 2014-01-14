/*******************************************************************************
 * Module with string comparison routines.  
 * Author: Walter dos Santos Filho - waltersf@gmail.com
 * Based on ...
 * TODO: Implement the following functions:
 *       - sortwinkler(), permwinkler(), bagdist() and bigram()
 */
#include <cstdlib>
#include <cctype>
#include <cstring>
#include <cstdio>
#include <cassert>
#include <algorithm>
#include "stringutil.h"

#define MAX_STRING_SIZE 1000
const unsigned short PREFIX = 4;

char *str_itoa( int value, char *string, int radix ){
    register unsigned mod_val;
    register unsigned div_val;
    register char    *address;
    char sign;

    if( radix >= ((int) 2U) && radix <= ((int) 36U) ) {
        if( ( sign = ( value < 0 ) ) ) {
            value = -value;
        }

        mod_val =   radix;
        div_val =   value;
        address = string + ( sizeof( unsigned ) << 3U );
        *address =  '\0';

        do {
            if( ( *--address  = (char)(div_val % mod_val ) + '0' ) > '9' ){
                *address += '@' - '9';
            }
        } while( div_val /= mod_val );

        if( sign ) {
            *--address = '-';
        }
        (void)strcpy( string, address );
    }
    return( string );
}

/**
 * str_itoa implemented using C++ (by tmacam).
 */
 
std::string str_itoa_cpp( int value, int radix )
{
	register unsigned mod_val;
	register unsigned div_val;
	char sign;

	std::string result; // Constructed in reverse order...
	char digit;	    // Current result digit

	if( radix >= ((int) 2U) && radix <= ((int) 36U) ) {
		if( ( sign = ( value < 0 ) ) ) {
			value = -value;
		}

		mod_val =   radix;
		div_val =   value;

		do {
			if( ( digit  = (char)(div_val % mod_val ) + '0' ) > '9' ){
				// Not a digit in 0..9 range, convert into
				// a letter in A..Z range
				digit += '@' - '9';
			}
			result.push_back(digit);
		} while( div_val /= mod_val );

		if( sign ) {
			digit = '-';
			result.push_back(digit);
		}
		
		// Fix the result - since it was written backwards
		std::reverse(result.begin(), result.end());
	}
	// FIXME throw runtime error if out of bounds

	return result;
}

/**
 * Counts how many transpositions are needed to transform <code>a</code> in 
 * <code>b</code>. The result is divided by 2.
 */
static int transpositions(char *a, char *b){
  int transpositions = 0;
  int i;
  for (i = 0; i < ((int) strlen(a)); i++) {
    if (a[i] != b[i]){ 
      transpositions ++;
    }
  }
  transpositions = transpositions/2;
  //printf("Transposicoes : %d\n" ,transpositions);
  return transpositions;
}

/**
 *
 */
static char * commonChars(const char * s, const char * t, int halflen){
  char common[MAX_STRING_SIZE], copy[MAX_STRING_SIZE];
  int i, j;
  char ch;
  short found = 0;
  int len_t;
  int common_size = 0;
  char *ret;
  
  strcpy(copy, t);
  memset(common, 0, MAX_STRING_SIZE);
  len_t = strlen(t);
 
  for(i = 0; i < ((int) strlen(s)); i++){
    ch = s[i];
    found = 0;
    int min = MIN(i + halflen, len_t);
    for(j = MAX(0,i - halflen); ! found && j < min; j++){
      if(copy[j] == ch){
        found = 1;
        common[common_size] = ch;
        common_size ++;
        copy[j] = '*';
      }
    }     
  }
  //plus 1 because need to put the \0 in the end of the string
  ret = (char *) malloc(sizeof(char) * common_size + 1);
  strncpy(ret, common, sizeof(char) * common_size + 1);
  return ret;   
}
/**
 * 
 */
static int commonPrefixLength(int maxLength, const char * common1, const char * common2){
  int i;  
  int n = (int) MIN(maxLength, (int) MIN(strlen(common1), strlen(common2)) );
  for (i = 0; i < n; i++) {
    if (common1[i] != common2[i]) return i;
  }
  return n; /* first n characters are the same */
}
static float winkler_score(const char *s, const char * t, int maxLength){
  return commonPrefixLength(maxLength, s, t);
}
int startsWith(char *s, char *p){
	int i;
	int m, n;
	assert(s);
	assert(p);
	m = strlen(s); n = strlen(p);
	if (m < n){
		return 0;
	}
	for (i = 0; i < n; i++){
		if (s[i] != p[i]){
			return 0;
		}
	}
	return 1;
}

char *trim(char *str) {
  char *s, *dst, *last = NULL;
  if (NULL == str){
    return NULL;
  }
  s = dst = str;
  while (*s && isspace(*s)) s++;
  if (!*s) {*str = '\0'; return str; }
  do {
    if (!isspace(*s)) last = dst;
    *dst++ = *s++;
  } while (*s);
  *(last+1) = '\0';
  return str;
}


float exact(char * s, char *t){
 return !strcmp(s, t); 
}
/**
 * Implementation of Jaro distance.
 * As desribed in 'An Application of the Fellegi-Sunter Model of Record Linkage 
 * to the 1990 U.S. Decennial Census' by William E. Winkler and Yves Thibaudeau.
 * @param s First string (can be a const)
 * @param t Second string (can be a const)
 * @return A value between 0.0 and 1.0 for the Jaro distance
 */
float jaro(const char *s, const char * t){
  if (strcmp(s, t) == 0) {
	  return 1.0f;
  }
  size_t ss1 = strlen(s);
  size_t ss2 = strlen(t);


  int halflen = (ss1 > ss2) ? ss1 / 2 + 1 : ss2 / 2 + 1;

  char *common1 = commonChars(s, t, halflen);
  char *common2 = commonChars(t, s, halflen);
  int transpos;
  float retval = 0.0F;
  
  size_t sc1 = strlen(common1);
  size_t sc2 = strlen(common2);

  if(sc1 == sc2 && sc1 != 0){
	transpos = transpositions(common1,common2);
	retval = (
			((float) sc1) / ss1 
		 +  ((float) sc2) / ss2
		 +  (sc1 - transpos)/ ((float)sc1)
		 )/3.0F;
  }
  free(common1);
  free(common2);
  return retval;
}
/**
 * As desribed in 'An Application of the Fellegi-Sunter Model of
 * Record Linkage to the 1990 U.S. Decennial Census' by William E. Winkler
 * and Yves Thibaudeau. Based on the 'jaro' string comparator, but modifies it 
 * according to wether the first few characters are the same or not.
 */
float winkler(const char *s, const char * t){
    //quick check if the strings are the same
    if (0 == strcmp(s, t)){
        return 1.0;    
    }
    float dist = jaro(s,t);
    float prefLength = winkler_score(s, t, PREFIX);
    dist = dist + prefLength * 0.1 * (1 - dist);
    return dist; 
}
float editdistance_score(const char *str1, const char *str2){
    int** d;
    int m, n;
    int i, j, cost;
    int row, compRow;
    m = strlen(str1);
    n = strlen(str2);
    /* Inicializa a matriz de distancias*/
    d = (int**) malloc(sizeof(int *) * 2);
    d[0] = (int *) malloc(sizeof(int) * (n + 1));
    d[1] = (int *) malloc(sizeof(int) * (n + 1));
    d[0][0] = 0;

    for(j = 1; j <= n; j++){    
        d[0][j] = j;
    }
    for (i = 1; i <= m; i++){
         d[i % 2][0] = i;
         row = i % 2;
         for(j = 1; j <= n; j++){
         	compRow = (i + 1) % 2;
            cost = (str1[i - 1] == str2[j - 1])?0:1; 
            d[row][j] = MIN3(d[compRow][j] + 1,
            				 d[row][j -1] + 1,
                			 d[compRow][j -1] + cost);
         }
    }
    float result = (MAX(n,m) - (float) d[m % 2][n])/(MAX(n,m));
    free(d[0]);
    free(d[1]);
    free(d);
    return result;
}
/**
 * 
 */
int bmh(char *t, char *p){
	int m = strlen(t);
	int n = strlen(p);
	int i, j, k;
	int ret = -1;
	int d[MAX_STRING_SIZE + 1];
	if (n > m){ return -1;}
	
	for (j =0; j < MAX_STRING_SIZE; j++){
		d[j] = n;
	}
	for (j = 1; j < n; j++){
		d[(int) p[j - 1]] = n - j;
	}
	
	i = n;
	while(i <= m && ret < 0){
		k = i; j = n;
		while (t[k - 1] == p[j - 1] && j > 0){
			k --; j --;
		}
		if (!j){
			ret = k + 1;
		}
		i += d[(int) t[i - 1]];
	}
	return ret;
}
int bmhAfter(char *t, char *p, int start){
   int ret;
   if (start > ((int) strlen(t)) || (ret = bmh((char *) (t + start), p)) == -1){
   	return -1;
   } 
   return start + ret;
}
/**
 *  Attention: don't use stack allocated variables or constants, use only heap
 * allocated pointers for the first parameter.
 */
void replaceAll(char **t, const char *p, const char *r, int pos){
	char * buffer;
	int i;
	int m = strlen(*t);
	int n = strlen(p);
	int o = strlen(r);
	if(o > n){
		buffer = (char *) calloc(sizeof(char), (m / n) * (o - n) + m);
	} else {
		buffer = (char *) calloc(sizeof(char), m + 1);
	}
	strcpy(buffer, *t); 
	while ((pos = bmhAfter(buffer, (char *) p, pos)) > 0){
		int j = 0;
		for (i = pos - 1; i < pos - 1 + MIN(n,o); i++){
			buffer[i] = r[j];
			j ++;
		}
		if (j < o) {//complete
			m += o - j;
			for (i = m + o - j; i > pos; i --){
				buffer[i + o - j] = buffer[i]; 
			}
			for (i = pos + j - 1; i < pos + o - 1; i ++){
				buffer[i] = r[j];
				j ++;
			}
			pos += o;
		} else if (j < n){ //remove
			for(i = pos + j -1; i < m; i++){
				buffer[i] = buffer[i + n - j];
			}
		}
	}
	t = (char **) realloc(*t, (strlen(buffer)  + 1) * sizeof(char));
	strcpy(*t, buffer);
	free (buffer);
}
/**
 * Return approximate string comparator measure (between 0.0 and 1.0)
 * using bigrams. 
 * Bigrams are two-character sub-strings contained in a string. For example,
 * 'peter' contains the bigrams: pe,et,te,er.  This routine counts the number of 
 * common bigrams and divides by the average number of bigrams. 
 * The resulting number is returned.
 */
 float bigram(char *s, char *t){
 	int i;
  	if (0 == strcmp(s, t)){
    	return 1.0;
  	}
  	int m = strlen(s);
  	int n = strlen(t);
  	if (m == 0 || n == 0){
  		return 0.0;
  	}
  	//The number of bigrams is the size of the string, minus 1
  	char *bigr1 = (char *) calloc(sizeof(char), (m - 1) * 2 - 1);  
  	char *bigr2 = (char *) calloc(sizeof(char), (n - 1) * 2 - 1);

  	//Make a list of bigrams for both strings
  	bigr1[0] = s[0];
  	for (i = 1; i < m; i++){ 
    	bigr1[i * 2 - 1] = s[i];
    	bigr1[i * 2] = s[i];
  	}  
  	bigr2[0] = t[0];
  	for (i = 1; i < n; i++){ 
    	bigr2[i * 2 - 1] = t[i];
    	bigr2[i * 2] = t[i];
  	}
  	//Compute average number of bigrams  
	float average = (m - 1 + n - 1)/2.0;
	if (average == 0.0){
    	return 0.0;
	}
	char * short_bigr;
	char *long_bigr;
  	float common = 0.0;
    if (strlen(bigr1) < strlen(bigr2)){ //Count using the shorter bigram list
    	short_bigr = bigr1;
    	long_bigr  = bigr2;
    } else { 
    	short_bigr = bigr2;
    	long_bigr  = bigr1;
    }
    char bigram[3];
    int pos; 
    bigram[2] = '\0';
    for (i = 0; i < ((int) strlen(short_bigr)); i *= 2){
    	bigram[0] = short_bigr[i]; bigram[1] = short_bigr[i + 1];
    	if ((pos = bmh(long_bigr, bigram)) > 0){
    		common ++;
    		//mark this bigram as counted
    		long_bigr[pos] = '\1'; long_bigr[pos + 1] = '\1';  
    	}
    }
  	return common / average;
}
int str_is_vowel(char ch){
	return (ch == 'a' || ch == 'e' || ch == 'i' || ch == 'o' || ch == 'u' ||
		ch == 'A' || ch == 'E' || ch == 'I' || ch == 'O' || ch == 'U');
}
