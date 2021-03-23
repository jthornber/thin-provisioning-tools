#include "dbg-lib/sm_show_traits.h"

using namespace dbg;
using namespace std;

//----------------------------------------------------------------

void
index_entry_show_traits::show(formatter::ptr f, string const &key,
			      persistent_data::sm_disk_detail::index_entry const &value)
{
	field(*f, "blocknr", value.blocknr_);
	field(*f, "nr_free", value.nr_free_);
	field(*f, "none_free_before", value.none_free_before_);
}

void
sm_root_show_traits::show(formatter::ptr f, string const &key,
			  persistent_data::sm_disk_detail::sm_root const &value)
{
	field(*f, "nr_blocks", value.nr_blocks_);
	field(*f, "nr_allocated", value.nr_allocated_);
	field(*f, "bitmap_root", value.bitmap_root_);
	field(*f, "ref_count_root", value.ref_count_root_);
}

//----------------------------------------------------------------
