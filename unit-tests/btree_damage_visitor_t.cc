#include "gmock/gmock.h"

#include "test_utils.h"

#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/endian_utils.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/transaction_manager.h"

using namespace std;
using namespace persistent_data;
using namespace test;
using namespace testing;

//----------------------------------------------------------------

namespace {
	block_address const BLOCK_SIZE = 4096;
	block_address const NR_BLOCKS = 102400;
	block_address const SUPERBLOCK = 0;

	struct thing {
		thing(uint32_t x_ = 0, uint64_t y_ = 0)
			: x(x_),
			  y(y_) {
		}

		bool operator ==(thing const &rhs) const {
			return (x == rhs.x) && (y == rhs.y);
		}

		uint32_t x;
		uint64_t y;
	};

	struct thing_disk {
		le32 x;
		le64 y;
	};

	struct thing_traits {
		typedef thing_disk disk_type;
		typedef thing value_type;
		typedef persistent_data::no_op_ref_counter<value_type> ref_counter;

		static void unpack(thing_disk const &disk, thing &value) {
			value.x = to_cpu<uint32_t>(disk.x);
			value.y = to_cpu<uint64_t>(disk.y);
		}

		static void pack(thing const &value, thing_disk &disk) {
			disk.x = to_disk<le32>(value.x);
			disk.y = to_disk<le64>(value.y);
		}
	};

	template <uint32_t Levels, typename ValueTraits>
	class btree_layout : public btree<Levels, ValueTraits>::visitor {
	public:
		struct node_info {
			typedef boost::shared_ptr<node_info> ptr;

			bool leaf;
			unsigned depth;
			unsigned level;
			block_address b;
			range<uint64_t> keys;
		};

		typedef btree_detail::node_location node_location;
		typedef btree<Levels, ValueTraits> tree;
		typedef boost::shared_ptr<btree_layout> ptr;

		virtual bool visit_internal(node_location const &loc,
					    typename tree::internal_node const &n) {
			record_node(false, loc, n);
			return true;
		}

		virtual bool visit_internal_leaf(node_location const &loc,
						 typename tree::internal_node const &n) {
			record_node(true, loc, n);
			return true;
		}


		virtual bool visit_leaf(node_location const &loc,
					typename tree::leaf_node const &n) {
			record_node(true, loc, n);
			return true;
		}

		virtual void visit_complete() {
		}

		vector<typename node_info::ptr> const &get_nodes() const {
			return nodes_;
		}

		node_info const &random_node() const {
			if (nodes_.empty())
				throw runtime_error("no nodes in btree");

			return *nodes_[::random() % nodes_.size()];
		}

	private:
		// We rely on the visit order being depth first, lowest to highest.
		template <typename N>
		void record_node(bool leaf, node_location const &loc, N const &n) {
			typename node_info::ptr ni(new node_info);

			ni->leaf = leaf;
			ni->depth = loc.depth;
			ni->level = loc.level;
			ni->b = n.get_location();

			if (n.get_nr_entries())
				ni->keys = range<uint64_t>(n.key_at(0));
			else {
				if (loc.key)
					ni->keys = range<uint64_t>(*loc.key);
				else
					ni->keys = range<uint64_t>();
			}

			if (last_node_at_depth_.size() > loc.depth) {
				typename node_info::ptr &last = last_node_at_depth_[loc.depth];

				last->keys.end_ = ni->keys.begin_;
				last_node_at_depth_[loc.depth] = ni;
			} else
				last_node_at_depth_.push_back(ni);

			nodes_.push_back(ni);
		}

		vector<typename node_info::ptr> nodes_;
		vector<typename node_info::ptr> last_node_at_depth_;
	};

	class value_visitor_mock {
	public:
		MOCK_METHOD1(visit, void(thing const &));
	};

	class damage_visitor_mock {
	public:
		MOCK_METHOD1(visit, void(btree_detail::damage const &));
	};

	class BTreeDamageVisitorTests : public Test {
	public:
		BTreeDamageVisitorTests()
			: bm_(create_bm<BLOCK_SIZE>(NR_BLOCKS)),
			  sm_(setup_core_map()),
			  tm_(new transaction_manager(bm_, sm_)),
			  tree_(new btree<1, thing_traits>(tm_, rc_)) {
		}

		space_map::ptr setup_core_map() {
			space_map::ptr sm(new core_map(NR_BLOCKS));
			sm->inc(SUPERBLOCK);
			return sm;
		}

		void commit() {
			block_manager<>::write_ref superblock(bm_->superblock(SUPERBLOCK));
		}

		void trash_block(block_address b) {
			::test::zero_block(bm_, b);
		}

		void insert_values(unsigned nr) {
			for (unsigned i = 0; i < nr; i++) {
				uint64_t key[1] = {i};
				thing value(i, i + 1234);

				tree_->insert(key, value);
			}
		}

		void expect_no_values() {
			EXPECT_CALL(value_visitor_, visit(_)).Times(0);
		}

		void expect_value_range(uint64_t begin, uint64_t end) {
			while (begin < end) {
				EXPECT_CALL(value_visitor_, visit(Eq(thing(begin, begin + 1234)))).Times(1);
				begin++;
			}
		}

		void expect_nr_values(unsigned nr) {
			expect_value_range(0, nr);
		}

		void expect_value(unsigned n) {
			EXPECT_CALL(value_visitor_, visit(Eq(thing(n, n + 1234)))).Times(1);
		}

		void expect_no_damage() {
			EXPECT_CALL(damage_visitor_, visit(_)).Times(0);
		}

		void expect_damage(unsigned level, range<uint64_t> keys) {
			EXPECT_CALL(damage_visitor_, visit(Eq(damage(level, keys, "foo")))).Times(1);
		}

		typedef typename btree_layout<1, thing_traits>::node_info node_info;

		vector<typename node_info::ptr> const &get_nodes() {
			if (!nodes_) {
				btree_layout<1, thing_traits> layout;
				tree_->visit_depth_first(layout);
				nodes_ = layout.get_nodes(); // expensive copy, but it's only a test
			}

			return *nodes_;
		}

		unsigned node_index_of_nth_leaf(vector<typename node_info::ptr> const &nodes,
						unsigned target) const {
			unsigned i;
			for (i = 0; i < nodes.size(); i++)
				if (nodes[i]->leaf) {
					if (!target)
						break;
					else
						target--;
				}

			if (target)
				throw runtime_error("not that many leaf nodes");

			return i;
		}

		unsigned get_nr_leaf_nodes(vector<typename node_info::ptr> const &nodes) {
			unsigned nr_leaf = 0;

			for (unsigned i = 0; i < nodes.size(); i++)
				if (nodes[i]->leaf)
					nr_leaf++;

			return nr_leaf;
		}

		node_info::ptr random_leaf_node() {
			vector<typename node_info::ptr> const &nodes = get_nodes();

			unsigned nr_leaf = get_nr_leaf_nodes(nodes);
			unsigned target = random() % nr_leaf;
			unsigned i = node_index_of_nth_leaf(nodes, target);

			return nodes[i];
		}

		vector<node_info::ptr> get_random_leaf_nodes(unsigned count) {
			vector<node_info::ptr> const &nodes = get_nodes();

			unsigned nr_leaf = get_nr_leaf_nodes(nodes);
			unsigned target = random() % (nr_leaf - count);
			unsigned i = node_index_of_nth_leaf(nodes, target);

			vector<node_info::ptr> v;

			for (; i < nodes.size() && count; i++) {
				if (nodes[i]->leaf) {
					count--;
					v.push_back(nodes[i]);
				}
			}

			return v;
		}

		void trash_blocks(vector<node_info::ptr> const &blocks) {
			for (unsigned i = 0; i < blocks.size(); i++)
				trash_block(blocks[i]->b);
		}

		void run() {
			// We must commit before we do the test to ensure
			// all the block numbers and checksums are written
			// to the btree nodes.
			commit();

			block_counter counter;
			btree_damage_visitor<value_visitor_mock, damage_visitor_mock, 1, thing_traits>
				visitor(counter, value_visitor_, damage_visitor_);
			tree_->visit_depth_first(visitor);
		}

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager::ptr tm_;

		thing_traits::ref_counter rc_;
		btree<1, thing_traits>::ptr tree_;

		optional<vector<typename node_info::ptr> > nodes_;


		value_visitor_mock value_visitor_;
		damage_visitor_mock damage_visitor_;
	};
}

//----------------------------------------------------------------

TEST_F(BTreeDamageVisitorTests, an_empty_tree)
{
	expect_no_values();
	expect_no_damage();

	run();
}

TEST_F(BTreeDamageVisitorTests, tree_with_a_trashed_root)
{
	trash_block(tree_->get_root());

	expect_no_values();
	expect_damage(0, range<uint64_t>(0ull));

	run();
}

TEST_F(BTreeDamageVisitorTests, populated_tree_with_no_damage)
{
	insert_values(10000);

	expect_nr_values(10000);
	expect_no_damage();

	run();
}

TEST_F(BTreeDamageVisitorTests, populated_tree_with_a_damaged_leaf_node)
{
	insert_values(10000);
	commit();

	node_info::ptr n = random_leaf_node();

	trash_block(n->b);
	expect_value_range(0, *n->keys.begin_);
	expect_value_range(*n->keys.end_, 10000);
	expect_damage(0, n->keys);

	run();
}

TEST_F(BTreeDamageVisitorTests, populated_tree_with_a_sequence_of_damaged_leaf_nodes)
{
	insert_values(10000);
	commit();

	unsigned const COUNT = 5;
	vector<node_info::ptr> nodes = get_random_leaf_nodes(COUNT);

	trash_blocks(nodes);
	block_address begin = *nodes[0]->keys.begin_;
	block_address end = *nodes[COUNT - 1]->keys.end_;

	expect_value_range(0, *nodes[0]->keys.begin_);
	expect_value_range(*nodes[COUNT - 1]->keys.end_, 10000);
	expect_damage(0, range<block_address>(begin, end));

	run();
}

TEST_F(BTreeDamageVisitorTests, damaged_first_leaf)
{
	insert_values(10000);
	commit();

	vector<typename node_info::ptr> const &nodes = get_nodes();

	unsigned target = 0;
	unsigned i = node_index_of_nth_leaf(nodes, target);

	block_address end = *nodes[i]->keys.end_;
	trash_block(nodes[i]->b);

	expect_damage(0, range<block_address>(0ull, end));
	expect_value_range(end, 10000);

	run();
}

TEST_F(BTreeDamageVisitorTests, damaged_last_leaf)
{
	insert_values(10000);
	commit();

	vector<typename node_info::ptr> const &nodes = get_nodes();

	unsigned nr_leaf = get_nr_leaf_nodes(nodes);
	unsigned target = nr_leaf - 1;
	unsigned i = node_index_of_nth_leaf(nodes, target);

	block_address begin = *nodes[i]->keys.begin_;
	trash_block(nodes[i]->b);

	expect_value_range(0, begin);
	expect_damage(0, range<block_address>(begin));

	run();
}

//----------------------------------------------------------------
