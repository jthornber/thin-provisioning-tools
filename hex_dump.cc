#include "hex_dump.h"

#include <iostream>
#include <iomanip>

using namespace std;

//----------------------------------------------------------------

void base::hex_dump(ostream &out, void const *data_, size_t len)
{
	unsigned char const *data = reinterpret_cast<unsigned char const *>(data_),
		*end = data + len;
	out << hex;

	while (data < end) {
		for (unsigned i = 0; i < 16 && data < end; i++, data++)
			out << setw(2) << setfill('0') << (unsigned) *data << " ";
		out << endl;
	}
	out << dec;
}

//----------------------------------------------------------------
