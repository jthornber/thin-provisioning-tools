#ifndef CRUNCHER_COMPRESSOR_H
#define CRUNCHER_COMPRESSOR_H

#include <stdexcept>
#include <vector>
#include <stdint.h>

//----------------------------------------------------------------

namespace cruncher {
	class compression_error : public std::runtime_error {
	public:
		explicit compression_error(std::string const &what)
			: std::runtime_error(what) {
		}
	};

	class compression_oos_error : public compression_error {
	public:
		explicit compression_oos_error(std::string const &what)
			: compression_error(what) {
		}
	};

	class compressor {
	public:
		compressor();

		struct mem_region {
			uint8_t *begin;
			uint8_t *end;

			unsigned size() const {
				return end - begin;
			}
		};

		unsigned compress(mem_region const &src,
				  mem_region &dest);

		// You should know how much space this will take
		void decompress(mem_region const &src, mem_region &dest);


	private:
		std::vector<uint32_t> compression_tables_;
	};
}

//----------------------------------------------------------------

#endif
