################################################################
## Copyright (C) 2011 Red Hat, Inc. All rights reserved.
##
## This file is part of the thin-provisioning-tools source.
##
## thin-provisioning-tools is free software: you can redistribute it
## and/or modify it under the terms of the GNU General Public License
## as published by the Free Software Foundation, either version 3 of
## the License, or (at your option) any later version.
##
## thin-provisioning-tools is distributed in the hope that it will be
## useful, but WITHOUT ANY WARRANTY; without even the implied warranty
## of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License along
## with thin-provisioning-tools.  If not, see
## <http://www.gnu.org/licenses/>.
################################################################

AC_PREREQ(2.61)

################################################################
dnl -- Process this file with autoconf to produce a configure script.
AC_INIT
# AC_CONFIG_HEADERS([configure.h])

################################################################################					     
dnl -- Setup the directory where autoconf has auxilary files								     
AC_CONFIG_AUX_DIR(autoconf)
AC_CANONICAL_TARGET([])
AC_PROG_CXX([g++])
AC_PROG_CC([gcc])
AC_LANG(C++)

################################################################
dnl -- Checks for programs.
AC_PROG_SED
AC_PROG_AWK
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_MKDIR_P
AC_PROG_INSTALL

################################################################################
dnl -- Prefix is /usr by default, the exec_prefix default is setup later
AC_PREFIX_DEFAULT(/usr)

AC_CHECK_HEADERS([expat.h \
	          iostream \
	          boost/bind.hpp \
		  boost/crc.hpp \
		  boost/intrusive/circular_list_algorithms.hpp \
		  boost/intrusive/rbtree_algorithms.hpp \
		  boost/lexical_cast.hpp \
		  boost/noncopyable.hpp \
		  boost/optional.hpp \
		  boost/shared_ptr.hpp \
		  boost/static_assert.hpp],
		  [], [AC_MSG_ERROR(bailing out)])

################################################################################
dnl -- Setup the ownership of the files
AC_MSG_CHECKING(file owner)
AC_ARG_WITH(user,
	    AC_HELP_STRING([--with-user=USER],
			   [set the owner of installed files [[USER=]]]),
	    OWNER=$withval)
AC_MSG_RESULT($OWNER)

if test x$OWNER != x; then
	INSTALL="$INSTALL -o $OWNER"
fi

################################################################################
dnl -- Setup the group ownership of the files
AC_MSG_CHECKING(group owner)
AC_ARG_WITH(group,
	    AC_HELP_STRING([--with-group=GROUP],
			   [set the group owner of installed files [[GROUP=]]]),
	    INSTALL_GROUP=$withval)
AC_MSG_RESULT($INSTALL_GROUP)

if test x$INSTALL_GROUP != x; then
        INSTALL="$INSTALL -g $INSTALL_GROUP"
fi

################################################################################
dnl -- Enable debugging
AC_MSG_CHECKING(whether to enable debugging)
AC_ARG_ENABLE(debug, AC_HELP_STRING([--enable-debug], [enable debugging]),
              DEBUG=$enableval, DEBUG=no)
AC_MSG_RESULT($DEBUG)

if test x$DEBUG = xyes; then
        CXXDEBUG_FLAG=-g
fi

################################################################################
dnl -- Override optimisation
AC_MSG_CHECKING(for C++ optimisation flag)
CXXOPTIMISE_FLAG="-O8"
AC_ARG_WITH(optimisation,
            AC_HELP_STRING([--with-optimisation=OPT],
                           [C++ optimisation flag [[OPT=-O8]]]),
            CXXOPTIMISE_FLAG=$withval)
AC_MSG_RESULT($CXXOPTIMISE_FLAG)

if test x$CXXOPTIMISE_FLAG = xyes; then
   CXXOPTIMISE_FLAG=-O8
fi

################################################################################
dnl -- Enable testing
AC_MSG_CHECKING(whether to enable unit testing)
AC_ARG_ENABLE(testing,
              AC_HELP_STRING(--enable-testing, [enable testing targets in the makefile]),
              TESTING=$enableval, TESTING=no)
AC_MSG_RESULT($TESTING)

################################################################################
dnl -- Check for getopt
AC_CHECK_HEADERS(getopt.h, AC_DEFINE([HAVE_GETOPTLONG], 1, [Define to 1 if getopt_long is available.]))

################################################################################
THIN_PROVISIONING_TOOLS_VERSION="\"`cat "$srcdir"/VERSION 2>/dev/null || echo Unknown`\""

VER=`cat "$srcdir"/VERSION`
RELEASE_DATE="\"`echo $VER | $SED 's/.* (//;s/).*//'`\""
VER=`echo "$VER" | $AWK '{print $1}'`
RELEASE="\"`echo "$VER" | $AWK -F '-' '{print $2}'`\""
VER=`echo "$VER" | $AWK -F '-' '{print $1}'`
VERSION_MAJOR=`echo "$VER" | $AWK -F '.' '{print $1}'`
VERSION_MINOR=`echo "$VER" | $AWK -F '.' '{print $2}'`
VERSION_PATCHLEVEL=`echo "$VER" | $AWK -F '[[(.]]' '{print $3}'`

################################################################
AC_SUBST(CXXDEBUG_FLAG)
AC_SUBST(CXXOPTIMISE_FLAG)
AC_SUBST(INSTALL)
AC_SUBST(prefix)
AC_SUBST(RELEASE_DATE)
AC_SUBST(RELEASE_DATE)
AC_SUBST(TESTING)
AC_SUBST(THIN_PROVISIONING_TOOLS_VERSION)

################################################################################
dnl -- First and last lines should not contain files to generate in order to
dnl -- keep utility scripts running properly
AC_CONFIG_FILES([
Makefile
unit-tests/Makefile
version.h
])
AC_OUTPUT
