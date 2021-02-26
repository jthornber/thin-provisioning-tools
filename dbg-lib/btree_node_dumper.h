#ifndef DBG_BTREE_NODE_DUMPER
#define DBG_BTREE_NODE_DUMPER

#include "dbg-lib/block_dumper.h"
#include "dbg-lib/simple_show_traits.h"
#include "persistent-data/data-structures/btree.h"

//----------------------------------------------------------------

namespace dbg {
	using persistent_data::block_manager;
	using persistent_data::btree_detail::node_ref;

	template <typename ShowTraits>
	class btree_node_dumper : public block_dumper {
	public:
		virtual void show(block_manager::read_ref &rr, std::ostream &out) {
			node_ref<uint64_traits> n = btree_detail::to_node<uint64_traits>(rr);
			if (n.get_type() == INTERNAL)
				btree_node_dumper<uint64_show_traits>::show_node(n, out);
			else {
				node_ref<typename ShowTraits::value_trait> n = btree_detail::to_node<typename ShowTraits::value_trait>(rr);
				show_node(n, out);
			}
		}

		static void show_node(node_ref<typename ShowTraits::value_trait> n, std::ostream &out) {
			formatter::ptr f = create_xml_formatter();

			field(*f, "csum", n.get_checksum());
			field(*f, "blocknr", n.get_block_nr());
			field(*f, "type", n.get_type() == INTERNAL ? "internal" : "leaf");
			field(*f, "nr_entries", n.get_nr_entries());
			field(*f, "max_entries", n.get_max_entries());
			field(*f, "value_size", n.get_value_size());

			for (unsigned i = 0; i < n.get_nr_entries(); i++) {
				formatter::ptr f2 = create_xml_formatter();
				field(*f2, "key", n.key_at(i));
				ShowTraits::show(f2, "value", n.value_at(i));
				f->child(boost::lexical_cast<std::string>(i), f2);
			}

			f->output(out, 0);
		}
	};

	template <typename ShowTraits>
	block_dumper::ptr
	create_btree_node_dumper() {
		return block_dumper::ptr(new btree_node_dumper<ShowTraits>());
	}
}

//----------------------------------------------------------------

#endif
