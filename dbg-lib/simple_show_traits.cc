#include "dbg-lib/simple_show_traits.h"

using namespace dbg;

//----------------------------------------------------------------

void
uint32_show_traits::show(formatter::ptr f, string const &key, uint32_t const &value)
{
	field(*f, key, boost::lexical_cast<string>(value));
}

void
uint64_show_traits::show(formatter::ptr f, string const &key, uint64_t const &value)
{
	field(*f, key, boost::lexical_cast<string>(value));
}

//----------------------------------------------------------------
