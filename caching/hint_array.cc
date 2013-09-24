#include "caching/hint_array.h"

using namespace boost;
using namespace caching;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	using namespace caching::hint_array_detail;

	// We've got into a bit of a mess here.  Templates are compile
	// time, and we don't know the hint width until run time.  We're
	// going to have to provide specialisation for all legal widths and
	// use the appropriate one.

#define all_widths \
	xx(4); xx(8); xx(12); xx(16); xx(20); xx(24); xx(28); xx(32);\
	xx(36); xx(40); xx(44); xx(48); xx(52); xx(56); xx(60); xx(64); \
	xx(68); xx(72); xx(76); xx(80); xx(84); xx(88); xx(92); xx(96); \
	xx(100); xx(104); xx(108); xx(112); xx(116); xx(120); xx(124); xx(128);

	template <uint32_t WIDTH>
	shared_ptr<array_base> mk_array(transaction_manager::ptr tm) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<array_base> r = typename ha::ptr(new ha(tm, typename traits::ref_counter()));

		return r;
	}

	shared_ptr<array_base> mk_array(transaction_manager::ptr tm, uint32_t width) {
		switch (width) {
#define xx(n)	case n:	return mk_array<n>(tm)

			all_widths
#undef xx
		}

		// never get here
		return shared_ptr<array_base>();
	}

	//--------------------------------

	template <uint32_t WIDTH>
	shared_ptr<array_base> mk_array(transaction_manager::ptr tm, block_address root, unsigned nr_entries) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<array_base> r = typename ha::ptr(new ha(tm, typename traits::ref_counter(), root, nr_entries));

		return r;
	}

	shared_ptr<array_base> mk_array(transaction_manager::ptr tm, uint32_t width, block_address root, unsigned nr_entries) {
		switch (width) {
#define xx(n)	case n:	return mk_array<n>(tm, root, nr_entries)
			all_widths
#undef xx
		}

		// never get here
		return shared_ptr<array_base>();
	}

	//--------------------------------

	template <uint32_t WIDTH>
	void get_hint(shared_ptr<array_base> base, unsigned index, vector<unsigned char> &data) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<ha> a = dynamic_pointer_cast<ha>(base);
		if (!a)
			throw runtime_error("internal error: couldn't cast hint array");

		data = a->get(index);
	}

	void get_hint_(uint32_t width, shared_ptr<array_base> base, unsigned index, vector<unsigned char> &data) {
		switch (width) {
#define xx(n) case n: return get_hint<n>(base, index, data)
			all_widths
#undef xx
		}
	}

	//--------------------------------

	template <uint32_t WIDTH>
	void set_hint(shared_ptr<array_base> base, unsigned index, vector<unsigned char> const &data) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<ha> a = dynamic_pointer_cast<ha>(base);
		if (!a)
			throw runtime_error("internal error: couldn't cast hint array");

		a->set(index, data);
	}

        void set_hint_(uint32_t width, shared_ptr<array_base> base,
		      unsigned index, vector<unsigned char> const &data) {
		switch (width) {
#define xx(n) case n: return set_hint<n>(base, index, data)
		all_widths
#undef xx
		}
	}

	//--------------------------------

	template <uint32_t WIDTH>
	void grow(shared_ptr<array_base> base, unsigned new_nr_entries, vector<unsigned char> const &value) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<ha> a = dynamic_pointer_cast<ha>(base);
		if (!a)
			throw runtime_error("internal error: couldn't cast hint array");
		a->grow(new_nr_entries, value);
	}

	void grow_(uint32_t width, shared_ptr<array_base> base,
		   unsigned new_nr_entries, vector<unsigned char> const &value)
	{
		switch (width) {
#define xx(n) case n: return grow<n>(base, new_nr_entries, value)
		all_widths
#undef xx
		}
	}
}

//----------------------------------------------------------------

hint_array::hint_array(tm_ptr tm, unsigned width)
	: width_(width),
	  impl_(mk_array(tm, width))
{
}

hint_array::hint_array(typename hint_array::tm_ptr tm, unsigned width,
		       block_address root, unsigned nr_entries)
	: width_(width),
	  impl_(mk_array(tm, width, root, nr_entries))
{
}

block_address
hint_array::get_root() const
{
	return impl_->get_root();
}

void
hint_array::get_hint(unsigned index, vector<unsigned char> &data) const
{
	get_hint_(width_, impl_, index, data);
}

void
hint_array::set_hint(unsigned index, vector<unsigned char> const &data)
{
	set_hint_(width_, impl_, index, data);
}

void
hint_array::grow(unsigned new_nr_entries, vector<unsigned char> const &value)
{
	grow_(width_, impl_, new_nr_entries, value);
}

//----------------------------------------------------------------
