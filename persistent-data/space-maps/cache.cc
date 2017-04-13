#include "persistent-data/space-maps/cache.h"

using namespace persistent_data;

namespace {
	class cache_map : public checked_space_map {
	public:
		typedef boost::shared_ptr<cache_map> ptr;

		// cache_size must be a power of 2
		cache_map(checked_space_map::ptr wrappee, unsigned cache_size)
			: sm_(wrappee),
			  mask_(cache_size - 1),
			  entries_(cache_size),
			  hits_(0),
			  total_(0),
			  fetches_(0),
			  writebacks_(0) {
		}

		~cache_map() {
			cerr << hits_ << "/" << total_ << " hits, "
			     << fetches_ << " fetches, "
			     << writebacks_ << " writebacks\n";
		}

		block_address get_nr_blocks() const {
			return sm_->get_nr_blocks();
		}

		block_address get_nr_free() const {
			// FIXME: only approximate
			return sm_->get_nr_free() - dirty_.size();
		}

		ref_t get_count(block_address b) const {
			entry const *e = fetch(b);
			if (e)
				return e->c;
			else
				return sm_->get_count(b);
		}

		void set_count(block_address b, ref_t c) {
			fetch_set(b, c);
		}

		void commit() {
			flush();
			sm_->commit();
		}

		void inc(block_address b) {
			fetch(b).c++;
		}

		void dec(block_address b) {
			fetch(b).c--;
		}

		maybe_block find_free(span_iterator &it) {
			flush();
			return sm_->find_free(it);
		}

		bool count_possibly_greater_than_one(block_address b) const {
			entry const *e = fetch(b);
			if (e)
				return e->c > 1;
			else
				return sm_->count_possibly_greater_than_one(b);
		}

		void extend(block_address extra_blocks) {
			flush();
			sm_->extend(extra_blocks);
		}

		// I'm assuming a commit has been run before calling these 3
		void count_metadata(block_counter &bc) const {
			check_no_dirty();
			sm_->count_metadata(bc);
		}

		size_t root_size() const {
			return sm_->root_size();
		}

		void copy_root(void *dest, size_t len) const {
			check_no_dirty();
			sm_->copy_root(dest, len);
		}

		checked_space_map::ptr clone() const {
			cerr << "in clone\n";
			return ptr(new cache_map(*this));
		}

	private:
		struct entry {
			entry() : b(0), c(0), valid(false) {}

			block_address b;
			ref_t c;
			bool valid:1;
		};

		static uint32_t hash_u32(uint32_t n) {
			unsigned const GOLDEN_RATIO_32 = 0x9e3779b9;
		        return GOLDEN_RATIO_32 * n;
		}

		uint16_t hash(block_address b) const {
			return (hash_u32(b) >> 8) & mask_;
		}

		void load_set(entry &e, block_address b, ref_t c) const {
			e.b = b;
			e.c = c;
			e.valid = true;
		}

		void load(entry &e, block_address b) const {
			load_set(e, b, sm_->get_count(b));
			fetches_++;
		}

		entry const *fetch(block_address b) const {
			unsigned h = hash(b);
			entry *e = &entries_[h];

			total_++;
			if (e->valid) {
				if (e->b == b) {
					hits_++;
					return e;

				} else if (!is_dirty(h)) {
					load(*e, b);
					return e;

				} else
					return NULL;

			} else {
				load(*e, b);
				return e;
			}
		}

		entry &fetch_set(block_address b, ref_t c) {
			unsigned h = hash(b);
			entry &e = entries_[h];

			total_++;
			if (e.valid) {
				if (e.b != b) {
					writeback(h);
					load_set(e, b, c);
				} else {
					e.c = c;
					hits_++;
				}

			} else
				load_set(e, b, c);

			set_dirty(h);
			return e;
		}

		entry &fetch(block_address b) {
			unsigned h = hash(b);
			entry &e = entries_[h];

			total_++;
			if (e.valid) {
				if (e.b != b) {
					writeback(h);
					load(e, b);
				} else
					hits_++;

			} else
				load(e, b);

			set_dirty(h);
			return e;
		}

		bool is_dirty(uint16_t h) const {
			auto const &it = dirty_.find(h);
			return it != dirty_.end();
		}

		void set_dirty(uint16_t h) {
			dirty_.insert(h);
		}

		void clear_dirty(uint16_t h) {
			dirty_.erase(dirty_.find(h));
		}

		void writeback_(uint16_t h) {
			entry const &e = entries_[h];
			sm_->set_count(e.b, e.c);
			writebacks_++;
		}

		void writeback(uint16_t h) {
			if (is_dirty(h)) {
				writeback_(h);
				clear_dirty(h);
			}
		}

		void flush() {
			for (auto const &h : dirty_)
				writeback_(h);
			dirty_.clear();
		}

		void check_no_dirty() const {
			if (dirty_.size())
				throw runtime_error("cache space map dirty");
		}

		checked_space_map::ptr sm_;
		unsigned mask_;
		mutable set<uint16_t> dirty_;
		mutable vector<entry> entries_;

		mutable unsigned hits_;
		mutable unsigned total_;
		mutable unsigned fetches_;
		mutable unsigned writebacks_;
	};
}

checked_space_map::ptr
persistent_data::create_cache_sm(checked_space_map::ptr wrappee, unsigned cache_size)
{
	return checked_space_map::ptr(new cache_map(wrappee, cache_size));
}

