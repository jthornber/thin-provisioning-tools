#include "dbg-lib/block_dumper.h"
#include "dbg-lib/output_formatter.h"
#include "persistent-data/data-structures/array_block.h"

//----------------------------------------------------------------

namespace dbg {
	using persistent_data::block_manager;
	using persistent_data::array_block;

	template <typename ShowTraits>
	class array_block_dumper : public block_dumper {
	public:
		array_block_dumper(typename ShowTraits::value_trait::ref_counter rc)
			: rc_(rc) {
		}

		virtual void show(block_manager::read_ref &rr, std::ostream &out) {
			rblock b(rr, rc_);
			show_array_entries(b, out);
		}

	private:
		typedef array_block<typename ShowTraits::value_trait, block_manager::read_ref> rblock;

		void show_array_entries(rblock const& b, std::ostream &out) {
			formatter::ptr f = create_xml_formatter();
			uint32_t nr_entries = b.nr_entries();

			field(*f, "max_entries", b.max_entries());
			field(*f, "nr_entries", nr_entries);
			field(*f, "value_size", b.value_size());

			for (unsigned i = 0; i < nr_entries; i++) {
				formatter::ptr f2 = create_xml_formatter();
				ShowTraits::show(f2, "value", b.get(i));
				f->child(boost::lexical_cast<std::string>(i), f2);
			}

			f->output(out, 0);
		}

		typename ShowTraits::value_trait::ref_counter rc_;
	};

	template <typename ShowTraits>
	block_dumper::ptr
	create_array_block_dumper(typename ShowTraits::value_trait::ref_counter rc) {
		return block_dumper::ptr(new array_block_dumper<ShowTraits>(rc));
	}
}

//----------------------------------------------------------------
