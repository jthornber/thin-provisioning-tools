#include "caching/hint_array.h"

using namespace boost;
using namespace caching;
using namespace caching::hint_array_damage;
using namespace persistent_data;

//----------------------------------------------------------------

namespace {
	template <uint32_t WIDTH>
	struct hint_traits {
		typedef unsigned char byte;
		typedef byte disk_type[WIDTH];
		typedef vector<byte> value_type;
		typedef no_op_ref_counter<value_type> ref_counter;

		// FIXME: slow copying for now
		static void unpack(disk_type const &disk, value_type &value) {
			for (unsigned byte = 0; byte < WIDTH; byte++)
				value.at(byte) = disk[byte];
		}

		static void pack(value_type const &value, disk_type &disk) {
			for (unsigned byte = 0; byte < WIDTH; byte++)
				disk[byte] = value.at(byte);
		}
	};


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
		default:
			throw runtime_error("invalid hint width");
		}

		// never get here
		return shared_ptr<array_base>();
	}

	//--------------------------------

	template <typename HA>
	shared_ptr<HA>
	downcast_array(shared_ptr<array_base> base) {
		shared_ptr<HA> a = dynamic_pointer_cast<HA>(base);
		if (!a)
			throw runtime_error("internal error: couldn't cast hint array");

		return a;
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
		default:
			throw runtime_error("invalid hint width");
		}

		// never get here
		return shared_ptr<array_base>();
	}

	//--------------------------------

	template <uint32_t WIDTH>
	void get_hint(shared_ptr<array_base> base, unsigned index, vector<unsigned char> &data) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<ha> a = downcast_array<ha>(base);
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

		shared_ptr<ha> a = downcast_array<ha>(base);
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

		shared_ptr<ha> a = downcast_array<ha>(base);
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

	//--------------------------------

	template <typename ValueType>
	struct no_op_visitor {
		virtual void visit(uint32_t index, ValueType const &v) {
		}
	};

	class ll_damage_visitor {
	public:
		ll_damage_visitor(damage_visitor &v)
		: v_(v) {
		}

		virtual void visit(array_detail::damage const &d) {
			v_.visit(missing_hints(d.desc_, d.lost_keys_));
		}

	private:
		damage_visitor &v_;
	};

	template <uint32_t WIDTH>
	void check_hints(shared_ptr<array_base> base, damage_visitor &visitor) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<ha> a = downcast_array<ha>(base);
		no_op_visitor<typename traits::value_type> nv;
		ll_damage_visitor ll(visitor);
		a->visit_values(nv, ll);
	}

	void check_hints_(uint32_t width, shared_ptr<array_base> base,
			  damage_visitor &visitor) {
		switch (width) {
#define xx(n) case n: check_hints<n>(base, visitor); break
			all_widths
#undef xx
		}
	}
}

//----------------------------------------------------------------

missing_hints::missing_hints(std::string const desc, run<uint32_t> const &keys)
	: damage(desc),
	  keys_(keys)
{
}

void
missing_hints::visit(damage_visitor &v) const
{
	v.visit(*this);
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

void
hint_array::check(hint_array_damage::damage_visitor &visitor)
{
	check_hints_(width_, impl_, visitor);
}

//----------------------------------------------------------------
