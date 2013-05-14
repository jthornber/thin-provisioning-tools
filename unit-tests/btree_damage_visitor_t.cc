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

		// To ensure we have fewer entries per leaf, and thus more internal nodes.
		char padding[200];
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

	//--------------------------------

	struct node_info {
		typedef boost::shared_ptr<node_info> ptr;

		bool leaf;
		unsigned depth;
		unsigned level;
		block_address b;
		range<uint64_t> keys;
	};

	typedef vector<node_info> node_array;
	typedef vector<node_info::ptr> node_ptr_array;

	class btree_layout {
	public:
		btree_layout(vector<node_info::ptr> const &ns)
			: nodes_(ns.size(), node_info()) {
			for (unsigned i = 0; i < ns.size(); i++)
				nodes_[i] = *ns[i];
		}

		static bool is_leaf(node_info const &n) {
			return n.leaf;
		}

		static bool is_internal(node_info const &n) {
			return !n.leaf;
		}

		template <typename Predicate>
		unsigned get_nr_nodes(Predicate const &pred) const {
			unsigned nr = 0;

			for (auto n : nodes_)
				if (pred(n))
					nr++;

			return nr;
		}

		template <typename Predicate>
		node_info get_node(unsigned target, Predicate const &pred) const {
			unsigned i = 0;

			for (auto n : nodes_) {
				if (pred(n)) {
					if (!target)
						break;
					else
						target--;
				}

				i++;
			}

			if (target)
				throw runtime_error("not that many nodes");

			return nodes_[i];
		}

		template <typename Predicate>
		node_info random_node(Predicate const &pred) const {
			unsigned nr = get_nr_nodes(pred);
			unsigned target = random() % nr;
			return get_node(target, pred);
		}

		template <typename Predicate>
		node_array get_random_nodes(unsigned count, Predicate const &pred) const {
			unsigned nr = get_nr_nodes(pred);
			unsigned target = count + random() % (nr - count);

			node_array v;

			for (auto n : nodes_) {
				if (!target)
					break;

				if (pred(n)) {
					if (target <= count)
						v.push_back(n);

					target--;
				}
			}

			return v;
		}

	private:
		node_array nodes_;
	};

	//--------------------------------

	template <uint32_t Levels, typename ValueTraits>
	class btree_layout_visitor : public btree<Levels, ValueTraits>::visitor {
	public:
		typedef btree_detail::node_location node_location;
		typedef btree<Levels, ValueTraits> tree;
		typedef boost::shared_ptr<btree_layout_visitor> ptr;

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

		btree_layout get_layout() {
			return btree_layout(nodes_);
		}

	private:
		// We rely on the visit order being depth first, lowest to highest.
		template <typename N>
		void record_node(bool leaf, node_location const &loc, N const &n) {
			node_info::ptr ni(new node_info);

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
				node_info::ptr &last = last_node_at_depth_[loc.depth];

				last->keys.end_ = ni->keys.begin_;
				last_node_at_depth_[loc.depth] = ni;
			} else
				last_node_at_depth_.push_back(ni);

			nodes_.push_back(ni);
		}

		node_ptr_array nodes_;
		node_ptr_array last_node_at_depth_;
	};

	//----------------------------------

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

		void tree_complete() {
			commit();
			discover_layout();
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

		optional<btree_layout> layout_;

		value_visitor_mock value_visitor_;
		damage_visitor_mock damage_visitor_;

	private:
		void commit() {
			block_manager<>::write_ref superblock(bm_->superblock(SUPERBLOCK));
		}

		void discover_layout() {
			btree_layout_visitor<1, thing_traits> visitor;
			tree_->visit_depth_first(visitor);
			layout_ = visitor.get_layout();
		}
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
	tree_complete();

	node_info n = layout_->random_node(btree_layout::is_leaf);

	trash_block(n.b);
	expect_value_range(0, *n.keys.begin_);
	expect_value_range(*n.keys.end_, 10000);
	expect_damage(0, n.keys);

	run();
}

TEST_F(BTreeDamageVisitorTests, populated_tree_with_a_sequence_of_damaged_leaf_nodes)
{
	insert_values(10000);
	tree_complete();

	unsigned const COUNT = 5;
	node_array nodes = layout_->get_random_nodes(COUNT, btree_layout::is_leaf);

	for (auto n : nodes)
		trash_block(n.b);

	block_address begin = *nodes[0].keys.begin_;
	block_address end = *nodes[COUNT - 1].keys.end_;

	expect_value_range(0, *nodes[0].keys.begin_);
	expect_value_range(*nodes[COUNT - 1].keys.end_, 10000);
	expect_damage(0, range<block_address>(begin, end));

	run();
}

TEST_F(BTreeDamageVisitorTests, damaged_first_leaf)
{
	insert_values(10000);
	tree_complete();

	node_info n = layout_->get_node(0, btree_layout::is_leaf);

	block_address end = *n.keys.end_;
	trash_block(n.b);

	expect_damage(0, range<block_address>(0ull, end));
	expect_value_range(end, 10000);

	run();
}

TEST_F(BTreeDamageVisitorTests, damaged_last_leaf)
{
	insert_values(10000);
	tree_complete();

	node_info n = layout_->get_node(
		layout_->get_nr_nodes(btree_layout::is_leaf) - 1,
		btree_layout::is_leaf);
	block_address begin = *n.keys.begin_;
	trash_block(n.b);

	expect_value_range(0, begin);
	expect_damage(0, range<block_address>(begin));

	run();
}

TEST_F(BTreeDamageVisitorTests, damaged_internal)
{
	insert_values(10000);
	tree_complete();

	node_info n = layout_->random_node(btree_layout::is_internal);

	optional<block_address> begin = n.keys.begin_;
	optional<block_address> end = n.keys.end_;

	trash_block(n.b);

	expect_value_range(0, *begin);
	expect_damage(0, range<block_address>(begin, end));

	if (end)
		expect_value_range(*end, 10000);

	run();
}

//----------------------------------------------------------------
