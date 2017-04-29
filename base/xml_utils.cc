#include "xml_utils.h"

#include "base/file_utils.h"
#include <fstream>
#include <iostream>
#include <sys/stat.h>

using namespace xml_utils;

//----------------------------------------------------------------

void
xml_parser::parse(std::string const &backup_file, bool quiet)
{
	file_utils::check_file_exists(backup_file);
	ifstream in(backup_file.c_str(), ifstream::in);

	std::unique_ptr<base::progress_monitor> monitor = create_monitor(quiet);

	size_t total = 0;
	size_t input_length = file_utils::get_file_length(backup_file);

	XML_Error error_code = XML_ERROR_NONE;
	while (!in.eof() && error_code == XML_ERROR_NONE) {
		char buffer[4096];
		in.read(buffer, sizeof(buffer));
		size_t len = in.gcount();
		int done = in.eof();

		// Do not throw while normally aborted by element handlers
		if (!XML_Parse(parser_, buffer, len, done) &&
		    (error_code = XML_GetErrorCode(parser_)) != XML_ERROR_ABORTED) {
			ostringstream out;
			out << "Parse error at line "
			    << XML_GetCurrentLineNumber(parser_)
			    << ":\n"
			    << XML_ErrorString(XML_GetErrorCode(parser_))
			    << endl;
			throw runtime_error(out.str());
		}

		total += len;
		monitor->update_percent(total * 100 / input_length);
	}
}

unique_ptr<base::progress_monitor>
xml_parser::create_monitor(bool quiet)
{
	if (!quiet && isatty(fileno(stdout)))
		return base::create_progress_bar("Restoring");
	else
		return base::create_quiet_progress_monitor();
}

//----------------------------------------------------------------

void
xml_utils::build_attributes(attributes &a, char const **attr)
{
	while (*attr) {
		char const *key = *attr;

		attr++;
		if (!*attr) {
			ostringstream out;
			out << "No value given for xml attribute: " << key;
			throw runtime_error(out.str());
		}

		char const *value = *attr;
		a.insert(make_pair(string(key), string(value)));
		attr++;
	}
}

//----------------------------------------------------------------
