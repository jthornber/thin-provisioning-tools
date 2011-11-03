#include "metadata_checker.h"

using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
	// As well as the standard btree checks, we build up a set of what
	// devices having mappings defined, which can later be cross
	// referenced with the details tree.  A separate block_counter is
	// used to later verify the data space map.
	class mapping_validator : public btree_checker<2, block_traits> {
	public:
		typedef boost::shared_ptr<mapping_validator> ptr;

		mapping_validator(block_counter &metadata_counter, block_counter &data_counter)
			: btree_checker<2, block_traits>(metadata_counter),
			  data_counter_(data_counter) {
		}

		// Sharing can only occur in level 1 nodes.
		// FIXME: not true once we start having held roots.
		bool visit_internal_leaf(unsigned level,
					 bool sub_root,
					 optional<uint64_t> key,
					 btree_detail::node_ref<uint64_traits> const &n) {

			bool r = btree_checker<2, block_traits>::visit_internal_leaf(level, sub_root, key, n);
			if (!r && level == 0) {
				throw runtime_error("unexpected sharing in level 0 of mapping tree.");
			}

			for (unsigned i = 0; i < n.get_nr_entries(); i++)
				devices_.insert(n.key_at(i));

			return r;
		}

		bool visit_leaf(unsigned level,
				bool sub_root,
				optional<uint64_t> key,
				btree_detail::node_ref<block_traits> const &n) {
			bool r = btree_checker<2, block_traits>::visit_leaf(level, sub_root, key, n);

			if (r)
				for (unsigned i = 0; i < n.get_nr_entries(); i++)
					data_counter_.inc(n.value_at(i).block_);

			return r;
		}

		set<uint64_t> const &get_devices() const {
			return devices_;
		}

	private:
		block_counter &data_counter_;
		set<uint64_t> devices_;
	};

	class details_validator : public btree_checker<1, device_details_traits> {
	public:
		typedef boost::shared_ptr<details_validator> ptr;

		details_validator(block_counter &counter)
			: btree_checker<1, device_details_traits>(counter) {
		}

		bool visit_leaf(unsigned level,
				bool sub_root,
				optional<uint64_t> key,
				btree_detail::node_ref<device_details_traits> const &n) {
			bool r = btree_checker<1, device_details_traits>::visit_leaf(level, sub_root, key, n);

			if (r)
				for (unsigned i = 0; i < n.get_nr_entries(); i++)
					devices_.insert(n.key_at(i));

			return r;
		}

		set<uint64_t> const &get_devices() const {
			return devices_;
		}

	private:
		set<uint64_t> devices_;
	};

	struct check_count : public space_map::iterator {
		check_count(string const &desc, block_counter const &expected)
			: bad_(false),
			  expected_(expected),
			  errors_(new error_set(desc)) {
		}

		virtual void operator() (block_address b, ref_t actual) {
			ref_t expected = expected_.get_count(b);

			if (actual != expected) {
				ostringstream out;
				out << b << ": was " << actual
				    << ", expected " << expected;
				errors_->add_child(out.str());
				bad_ = true;
			}
		}

		bool bad_;
		block_counter const &expected_;
		error_set::ptr errors_;
	};

	optional<error_set::ptr>
	check_ref_counts(string const &desc, block_counter const &counts,
			 space_map::ptr sm) {

		check_count checker(desc, counts);
		sm->iterate(checker);
		return checker.bad_ ? optional<error_set::ptr>(checker.errors_) : optional<error_set::ptr>();
	}
}


//----------------------------------------------------------------

boost::optional<error_set::ptr>
thin_provisioning::metadata_check(metadata::ptr md)
{
	error_set::ptr errors(new error_set("Errors in metadata"));

	block_counter metadata_counter, data_counter;

	mapping_validator::ptr mv(new mapping_validator(metadata_counter,
							data_counter));
	md->mappings_->visit(mv);

	set<uint64_t> const &mapped_devs = mv->get_devices();
	details_validator::ptr dv(new details_validator(metadata_counter));
	md->details_->visit(dv);

	set<uint64_t> const &details_devs = dv->get_devices();

	for (set<uint64_t>::const_iterator it = mapped_devs.begin(); it != mapped_devs.end(); ++it)
		if (details_devs.count(*it) == 0) {
			ostringstream out;
			out << "mapping exists for device " << *it
			    << ", yet there is no entry in the details tree.";
			throw runtime_error(out.str());
		}

	metadata_counter.inc(SUPERBLOCK_LOCATION);
	md->metadata_sm_->check(metadata_counter);

	md->data_sm_->check(metadata_counter);
	errors->add_child(check_ref_counts("Errors in metadata block reference counts",
					   metadata_counter, md->metadata_sm_));
	errors->add_child(check_ref_counts("Errors in data block reference counts",
					   data_counter, md->data_sm_));

	return (errors->get_children().size() > 0) ?
		optional<error_set::ptr>(errors) :
		optional<error_set::ptr>();
}

//----------------------------------------------------------------
