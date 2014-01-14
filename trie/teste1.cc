#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <iostream>
using namespace std;
typedef struct teste {
	union {
		int id_int;
		struct {
			short id1;
			short id2;
		} s;
	};
} teste_t;
using namespace std;
int maina(int argc, char **argv) {
	int uncompressed[] = { 1, 2, 7, 20, 30, 280, 285, 284, 232991, 12};
	vector<short> compressed;
	int outPos = 0, previous = 0;
	int inPos;
	for (inPos = 0; inPos < 10; inPos++) {
		int delta = uncompressed[inPos] - previous;
		short signal = 1;
		if (delta < 0){
			delta *= -1;
			signal = -1;
		}
		previous = uncompressed[inPos];
		while (delta >= 128) {
			compressed.push_back(signal * (delta & 127) | 128);
			outPos++;
			printf("%d\t", (delta & 127) | 128);
			delta = delta >> 7;
		}
		compressed.push_back(delta);
		outPos++;
		printf("%d\t", delta);
	}
	printf("\n");
	int n = outPos -1;
	printf("Gastou %d bytes\n", outPos);
	outPos = 0;
	previous = 0;
	inPos = 0;
	int size = compressed.size();
	for (outPos = 0; outPos < size && inPos < size	; outPos++) {
		int shift;

		for (shift = 0;inPos < size; shift += 7) {
			int temp = compressed[inPos++];
			short signal = 1;
			if (temp < 0){
				signal = -1;
				temp *= -1;
			}
			previous += signal * ((temp & 127) << shift);
			if (temp < 128)
				break;
		}
		uncompressed[outPos] = previous;
		printf("%d\t", uncompressed[outPos]);
		fflush(stdout);

	}
}
class ByteAlignedIntegerCollection {
private:
	vector<unsigned char> store;
	int previousInEnum;
	int lastInserted;
	int total;
	int current;
public:
	void Add(int number){
		unsigned char first = 0;
		int delta = number - lastInserted;
		unsigned bytesNeeded = 1;
		if (delta < 0){
			delta *= -1;
			first |= 0x80; //set signal flag as negative offset
		}
		if (delta <= 15){
			first |= 0x0; //Flag as just one byte needed
			first |= delta;
			store.push_back(first);
		} else {
			first |= 15;
			delta -= 15;
			store.push_back(first);
			int pos = store.size() - 1;

			while (delta > 0){
				store.push_back(delta & 255);
				delta = delta >> 8;
				bytesNeeded ++;
			}
			store[pos] |= (bytesNeeded -1) << 4;
			if (bytesNeeded > 5){
				cout << lastInserted << " " << number << endl << flush;
			}
			assert(bytesNeeded <=5);
		}
		total ++;
		lastInserted = number;
	}
	void MoveToStart(){
		current = 0;
		previousInEnum = 0;
	}
	int GetNext() {
		unsigned char first = store[current ++];
		char signal = (first & 0x80) >> 7; //1000 0000
		int bytes = ((first & 0x70) >> 4); //0110 0000
		int offset = first & 0x0F; //0000 1111

		int shift = 0;
		while (bytes --){
			offset += (store[current ++] << shift);
			shift += 8;
		}

		previousInEnum = (signal?-1:1) * offset + previousInEnum;
		return previousInEnum;
	}
	int GetTotal(){
		return total;
	}
	ByteAlignedIntegerCollection(){
		lastInserted = 0;
		MoveToStart();
	}
	int GetTotalOfElementsInVector(){
		return store.size();
	}
	void Clear(){
		store.clear();
		MoveToStart();
		total = 0;
		lastInserted = 0;
	}
};
int main(int argc, char **argv) {
	ByteAlignedIntegerCollection b;
	vector<int> conferencia;

	for(int i = 0; i < 10000; i ++){
		conferencia.clear();
		b.Clear();
/*
		conferencia.push_back(100000);
		conferencia.push_back(34);
		conferencia.push_back(342344);
		conferencia.push_back(122);
		conferencia.push_back(212323);
		conferencia.push_back(0);*/

		srand(time(NULL) * abs(rand()) );
		int total = 1000;
		for (int j = 0; j < total; j ++){
			int n = abs(rand()) % 100;
			conferencia.push_back(n);
			b.Add(conferencia[j]);
		}
		//cout << "Para armazenar " << sizeof(int) * total << " gastou " << b.GetTotalOfElementsInVector() << endl << flush;
		for(int j = 0; j < conferencia.size(); j++){
			//cout << " Valor " << b.GetNext() << endl;
			//if (b.GetNext() != conferencia[j]){
			//	cout << " Valor " << b.GetNext() <<  " " << conferencia[j] << endl << flush;
			//}
			assert(b.GetNext() == conferencia[j]);
		}
	}
	/*
	b.MoveToStart();
	for(int i = 0; i < b.GetTotal(); i++){
		cout << " Valor " << b.GetNext() << endl;
	}
	cout << b.GetTotalOfElementsInVector() << endl;*/
}
