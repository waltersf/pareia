/*
 * AnthillUtils.cpp
 *
 *  Created on: 12/08/2009
 *      Author: walter
 */

#include "anthillutil.h"
int AnthillUtil::Close(AnthillOutputPort port) {
	return dsCloseOutputPort(port.out);
}

AnthillInputPort AnthillUtil::GetInputPort(string name) {
	return AnthillInputPort(name, dsGetInputPortByName(name.c_str()));
}
AnthillOutputPort AnthillUtil::GetOutputPort(string name) {
	return AnthillOutputPort(name, dsGetOutputPortByName(name.c_str()));
}
int AnthillUtil::Probe(AnthillInputPort port) {
	return dsProbe(port.in);
}
int AnthillUtil::Read(AnthillInputPort port, void *msg, int size) {
	return dsReadBuffer(port.in, msg, size);
}
int AnthillUtil::Send(AnthillOutputPort port, void *msg, int size) {
	return dsWriteBuffer(port.out, msg, size);
}
int AnthillUtil::Rank() {
	return dsGetMyRank();
}
int AnthillUtil::TotalOfReaders(AnthillOutputPort port) {
	return dsGetNumReaders(port.out);
}
int AnthillUtil::TotalOfWriters(AnthillInputPort port) {
	return dsGetNumWriters(port.in);
}
int AnthillUtil::TotalOfFilterInstances() {
	return dsGetTotalInstances();
}
