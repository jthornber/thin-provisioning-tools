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
			value.resize(WIDTH);
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
	xx(4);

	template <uint32_t WIDTH>
	shared_ptr<array_base> mk_array(transaction_manager &tm) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<array_base> r = typename ha::ptr(new ha(tm, typename traits::ref_counter()));

		return r;
	}

	shared_ptr<array_base> mk_array(transaction_manager &tm, uint32_t width) {
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
	shared_ptr<array_base> mk_array(transaction_manager &tm, block_address root, unsigned nr_entries) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<array_base> r = typename ha::ptr(new ha(tm, typename traits::ref_counter(), root, nr_entries));

		return r;
	}

	shared_ptr<array_base> mk_array(transaction_manager &tm, uint32_t width, block_address root, unsigned nr_entries) {
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

	class value_adapter {
	public:
		value_adapter(hint_visitor &v)
		: v_(v) {
		}

		virtual void visit(uint32_t index, std::vector<unsigned char> const &v) {
			v_.visit(static_cast<block_address>(index), v);
		}

	private:
		hint_visitor &v_;
	};

	struct no_op_visitor : public hint_visitor {
		virtual void visit(block_address cblock, std::vector<unsigned char> const &v) {
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
	void walk_hints(shared_ptr<array_base> base, hint_visitor &hv, damage_visitor &dv) {
		typedef hint_traits<WIDTH> traits;
		typedef array<traits> ha;

		shared_ptr<ha> a = downcast_array<ha>(base);
		value_adapter vv(hv);
		ll_damage_visitor ll(dv);
		a->visit_values(vv, ll);
	}

	void walk_hints_(uint32_t width, shared_ptr<array_base> base,
			 hint_visitor &hv, damage_visitor &dv) {
		switch (width) {
#define xx(n) case n: walk_hints<n>(base, hv, dv); break
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

hint_array::hint_array(transaction_manager &tm, unsigned width)
	: width_(check_width(width)),
	  impl_(mk_array(tm, width))
{
}

hint_array::hint_array(transaction_manager &tm, unsigned width,
		       block_address root, unsigned nr_entries)
	: width_(check_width(width)),
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
hint_array::walk(hint_visitor &hv, hint_array_damage::damage_visitor &dv)
{
	walk_hints_(width_, impl_, hv, dv);
}

void
hint_array::check(hint_array_damage::damage_visitor &visitor)
{
	no_op_visitor vv;
	walk(vv, visitor);
}

uint32_t
hint_array::check_width(uint32_t width)
{
	if (width % 4 || width == 0 || width > 128) {
		ostringstream msg;
		msg << "invalid hint width: " << width;
		throw runtime_error(msg.str());
	}

	return width;
}

//----------------------------------------------------------------
