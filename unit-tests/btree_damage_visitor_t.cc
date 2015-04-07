#include "gmock/gmock.h"

#include "test_utils.h"

#include "base/endian_utils.h"
#include "persistent-data/data-structures/btree_damage_visitor.h"
#include "persistent-data/space-maps/core.h"
#include "persistent-data/transaction_manager.h"
#include "persistent-data/run.h"

using namespace base;
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

	ostream &operator <<(ostream &out, thing const &t) {
		return out << "thing [" << t.x << ", " << t.y << "]";
	}

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

		btree_detail::btree_path path;

		bool leaf;
		unsigned depth;
		block_address b;
		run<uint64_t> keys;
	};

	ostream &operator <<(ostream &out, node_info const &ni) {
		out << "node_info [leaf = " << ni.leaf
		    << ", depth " << ni.depth
		    << ", path [";

		bool first = true;
		btree_detail::btree_path::const_iterator it;
		for (it = ni.path.begin(); it != ni.path.end(); ++it) {
			if (first)
				first = false;
			else
				out << ", ";

			out << *it;
		}

		out << "], b " << ni.b
		    << ", keys " << ni.keys
		    << "]";

		return out;
	}

	bool is_leaf(node_info const &n) {
		return n.leaf;
	}

	bool is_internal(node_info const &n) {
		return !n.leaf;
	}

	typedef vector<node_info> node_array;
	typedef vector<node_info::ptr> node_ptr_array;

	class btree_layout {
	public:
		btree_layout(vector<node_info::ptr> const &ns)
			: nodes_(ns.size(), node_info()) {
			for (unsigned i = 0; i < ns.size(); i++)
				nodes_[i] = *ns[i];
		}

		template <typename Predicate>
		unsigned get_nr_nodes(Predicate const &pred) const {
			unsigned nr = 0;

			node_array::const_iterator it;
			for (it = nodes_.begin(); it != nodes_.end(); ++it)
				if (pred(*it))
					nr++;

			return nr;
		}

		template <typename Predicate>
		node_info get_node(unsigned target, Predicate const &pred) const {
			unsigned i = 0;

			node_array::const_iterator it;
			for (it = nodes_.begin(); it != nodes_.end(); ++it) {
				if (pred(*it)) {
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

			node_array::const_iterator it;
			for (it = nodes_.begin(); it != nodes_.end(); ++it) {
				if (!target)
					break;

				if (pred(*it)) {
					if (target <= count)
						v.push_back(*it);

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

			ni->path = loc.path;
			ni->leaf = leaf;
			ni->depth = loc.depth;
			ni->b = n.get_location();

			if (n.get_nr_entries())
				ni->keys = run<uint64_t>(n.key_at(0));
			else {
				if (loc.key)
					ni->keys = run<uint64_t>(*loc.key);
				else
					ni->keys = run<uint64_t>();
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

	MATCHER_P(DamagedKeys, keys, "") {
		return arg.lost_keys_ == keys;
	}

	MATCHER(EmptyPath, "") {
		return arg == btree_path();
	}

	class value_visitor_mock {
	public:
		MOCK_METHOD2(visit, void(btree_path const &, thing const &));
	};

	class damage_visitor_mock {
	public:
		MOCK_METHOD2(visit, void(btree_path const &, btree_detail::damage const &));
	};

	class DamageTests : public Test {
	public:
		DamageTests()
			: bm_(create_bm<BLOCK_SIZE>(NR_BLOCKS)),
			  sm_(setup_core_map()),
			  tm_(bm_, sm_) {
		}

		virtual ~DamageTests() {}

		void tree_complete() {
			commit();
			discover_layout();
		}

		void run() {
			commit();
			run_();
		}

		void trash_block(block_address b) {
			::test::zero_block(bm_, b);
		}

		//--------------------------------

		void expect_no_values() {
			EXPECT_CALL(value_visitor_, visit(_, _)).Times(0);
		}

		void expect_no_damage() {
			EXPECT_CALL(damage_visitor_, visit(_, _)).Times(0);
		}

		//--------------------------------

		with_temp_directory dir_;
		block_manager<>::ptr bm_;
		space_map::ptr sm_;
		transaction_manager tm_;
		thing_traits::ref_counter rc_;

		boost::optional<btree_layout> layout_;

		value_visitor_mock value_visitor_;
		damage_visitor_mock damage_visitor_;

	private:
		space_map::ptr setup_core_map() {
			space_map::ptr sm(new core_map(NR_BLOCKS));
			sm->inc(SUPERBLOCK);
			return sm;
		}

		void commit() {
			block_manager<>::write_ref superblock(bm_->superblock(SUPERBLOCK));
		}

		virtual void discover_layout() = 0;
		virtual void run_() = 0;
	};

	//--------------------------------

	class BTreeDamageVisitorTests : public DamageTests {
	public:
		BTreeDamageVisitorTests()
			: tree_(new btree<1, thing_traits>(tm_, rc_)) {
		}

		void insert_values(unsigned nr) {
			for (unsigned i = 0; i < nr; i++) {
				uint64_t key[1] = {i};
				thing value(i, i + 1234);

				tree_->insert(key, value);
			}
		}

		void expect_value_run(uint64_t begin, uint64_t end) {
			while (begin < end) {
				btree_path path;
				path.push_back(begin);
				EXPECT_CALL(value_visitor_, visit(Eq(path), Eq(thing(begin, begin + 1234)))).Times(1);
				begin++;
			}
		}

		void expect_nr_values(unsigned nr) {
			expect_value_run(0, nr);
		}

		void expect_value(uint64_t n) {
			btree_path path;
			path.push_back(n);
			EXPECT_CALL(value_visitor_, visit(Eq(path), Eq(thing(n, n + 1234)))).Times(1);
		}

		void expect_damage(base::run<uint64_t> const &keys) {
			EXPECT_CALL(damage_visitor_, visit(EmptyPath(), DamagedKeys(keys))).Times(1);
		}

		btree<1, thing_traits>::ptr tree_;

	private:
		virtual void discover_layout() {
			btree_layout_visitor<1, thing_traits> visitor;
			tree_->visit_depth_first(visitor);
			layout_ = visitor.get_layout();
		}

		virtual void run_() {
			btree_visit_values(*tree_, value_visitor_, damage_visitor_);
		}
	};

	//--------------------------------

	// 2 level btree
	class BTreeDamageVisitor2Tests : public DamageTests {
	public:
		BTreeDamageVisitor2Tests()
			: tree_(new btree<2, thing_traits>(tm_, rc_)) {
		}

		void insert_values(unsigned nr_sub_trees, unsigned nr_values) {
			for (unsigned i = 0; i < nr_sub_trees; i++)
				insert_sub_tree_values(i, nr_values);
		}

		void insert_sub_tree_values(unsigned sub_tree, unsigned nr_values) {
			for (unsigned i = 0; i < nr_values; i++) {
				uint64_t key[2] = {sub_tree, i};
				thing value(key_to_value(key));
				tree_->insert(key, value);
			}
		}

		void expect_values(unsigned nr_sub_trees, unsigned nr_values) {
			for (unsigned i = 0; i < nr_sub_trees; i++)
				expect_sub_tree_values(i, nr_values);
		}

		void expect_sub_tree_values(unsigned sub_tree, unsigned nr_values) {
			for (unsigned i = 0; i < nr_values; i++) {
				uint64_t key[2] = {sub_tree, i};
				btree_path path;
				path.push_back(sub_tree);
				path.push_back(i);
				EXPECT_CALL(value_visitor_, visit(Eq(path),
								  Eq(key_to_value(key))));
			}
		}

		void expect_values_except(unsigned nr_sub_trees, unsigned nr_values,
					  btree_path const &path, base::run<uint64_t> keys) {
			for (unsigned i = 0; i < nr_sub_trees; i++)
				expect_sub_tree_values_except(i, nr_values, path, keys);
		}

		void expect_sub_tree_values_except(unsigned sub_tree, unsigned nr_values,
						   btree_path const &path, base::run<uint64_t> keys) {
			for (unsigned i = 0; i < nr_values; i++) {
				uint64_t key[2] = {sub_tree, i};

				if (sub_tree == path[0] && keys.contains(i))
					continue;

				btree_path p2;
				p2.push_back(sub_tree);
				p2.push_back(i);
				EXPECT_CALL(value_visitor_, visit(Eq(p2), Eq(key_to_value(key))));
 			}
 		}

		void expect_damage(btree_path const &path, base::run<uint64_t> const &keys) {
			EXPECT_CALL(damage_visitor_, visit(Eq(path), DamagedKeys(keys))).Times(1);
		}

		btree<2, thing_traits>::ptr tree_;

	private:
		thing key_to_value(uint64_t key[2]) {
			return thing(key[0] * 1000000 + key[1], key[1] + 1234);
		}

		virtual void discover_layout() {
			btree_layout_visitor<2, thing_traits> visitor;
			tree_->visit_depth_first(visitor);
			layout_ = visitor.get_layout();
		}

		virtual void run_() {
			btree_visit_values(*tree_, value_visitor_, damage_visitor_);
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
	expect_damage(base::run<uint64_t>(0ull));

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

	node_info n = layout_->random_node(is_leaf);

	trash_block(n.b);
	expect_value_run(0, *n.keys.begin_);
	expect_value_run(*n.keys.end_, 10000);
	expect_damage(n.keys);

	run();
}

TEST_F(BTreeDamageVisitorTests, populated_tree_with_a_sequence_of_damaged_leaf_nodes)
{
	insert_values(10000);
	tree_complete();

	unsigned const COUNT = 5;
	node_array nodes = layout_->get_random_nodes(COUNT, is_leaf);

	node_array::const_iterator it;
	for (it = nodes.begin(); it != nodes.end(); ++it)
		trash_block(it->b);

	block_address begin = *nodes[0].keys.begin_;
	block_address end = *nodes[COUNT - 1].keys.end_;

	expect_value_run(0, *nodes[0].keys.begin_);
	expect_value_run(*nodes[COUNT - 1].keys.end_, 10000);
	expect_damage(base::run<block_address>(begin, end));

	run();
}

TEST_F(BTreeDamageVisitorTests, damaged_first_leaf)
{
	insert_values(10000);
	tree_complete();

	node_info n = layout_->get_node(0, is_leaf);

	block_address end = *n.keys.end_;
	trash_block(n.b);

	expect_damage(base::run<block_address>(0ull, end));
	expect_value_run(end, 10000);

	run();
}

TEST_F(BTreeDamageVisitorTests, damaged_last_leaf)
{
	insert_values(10000);
	tree_complete();

	node_info n = layout_->get_node(
		layout_->get_nr_nodes(is_leaf) - 1,
		is_leaf);
	block_address begin = *n.keys.begin_;
	trash_block(n.b);

	expect_value_run(0, begin);
	expect_damage(base::run<block_address>(begin));

	run();
}

TEST_F(BTreeDamageVisitorTests, damaged_internal)
{
	insert_values(10000);
	tree_complete();

	node_info n = layout_->random_node(is_internal);

	boost::optional<block_address> begin = n.keys.begin_;
	boost::optional<block_address> end = n.keys.end_;

	trash_block(n.b);

	expect_value_run(0, *begin);
	expect_damage(base::run<block_address>(begin, end));

	if (end)
		expect_value_run(*end, 10000);

	run();
}

//----------------------------------------------------------------

TEST_F(BTreeDamageVisitor2Tests, empty_tree)
{
	tree_complete();
	expect_no_damage();
	expect_no_values();

	run();
}

TEST_F(BTreeDamageVisitor2Tests, tree_with_a_trashed_root)
{
	tree_complete();
	trash_block(tree_->get_root());

	expect_no_values();

	btree_path path;
	expect_damage(path, base::run<uint64_t>(0ull));

	run();
}

TEST_F(BTreeDamageVisitor2Tests, populated_tree_with_no_damage)
{
	insert_values(10, 10);
	tree_complete();

	expect_values(10, 10);
	expect_no_damage();

	run();
}

namespace {
	bool leaf1(node_info const &n) {
		return (n.leaf && n.path.size() == 1 && n.path[0] == 1);
	}
}

TEST_F(BTreeDamageVisitor2Tests, damaged_leaf)
{
	insert_values(10, 1000);
	tree_complete();

	node_info n = layout_->random_node(leaf1);
	trash_block(n.b);

	expect_damage(n.path, n.keys);
	expect_values_except(10, 1000, n.path, n.keys);

	run();
}

//----------------------------------------------------------------
