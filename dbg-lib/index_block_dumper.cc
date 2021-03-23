#include "dbg-lib/index_block_dumper.h"
#include "dbg-lib/output_formatter.h"
#include "dbg-lib/sm_show_traits.h"
#include "persistent-data/space-maps/disk_structures.h"

using namespace dbg;

//----------------------------------------------------------------

namespace {
	using persistent_data::block_manager;

	class index_block_dumper : public dbg::block_dumper {
	public:
		virtual void show(block_manager::read_ref &rr, std::ostream &out) {
			sm_disk_detail::metadata_index const *mdi =
				reinterpret_cast<sm_disk_detail::metadata_index const *>(rr.data());
			show_metadata_index(mdi, sm_disk_detail::MAX_METADATA_BITMAPS, out);
		}

	private:
		void show_metadata_index(sm_disk_detail::metadata_index const *mdi, unsigned nr_indexes, std::ostream &out) {
			formatter::ptr f = create_xml_formatter();
			field(*f, "csum", to_cpu<uint32_t>(mdi->csum_));
			field(*f, "padding", to_cpu<uint32_t>(mdi->padding_));
			field(*f, "blocknr", to_cpu<uint64_t>(mdi->blocknr_));

			sm_disk_detail::index_entry ie;
			for (unsigned i = 0; i < nr_indexes; i++) {
				sm_disk_detail::index_entry_traits::unpack(*(mdi->index + i), ie);

				if (!ie.blocknr_ && !ie.nr_free_ && !ie.none_free_before_)
					continue;

				formatter::ptr f2 = create_xml_formatter();
				index_entry_show_traits::show(f2, "value", ie);
				f->child(boost::lexical_cast<string>(i), f2);
			}
			f->output(out, 0);
		}
	};
}

//----------------------------------------------------------------

block_dumper::ptr
dbg::create_index_block_dumper() {
	return block_dumper::ptr(new index_block_dumper());
}

//----------------------------------------------------------------
