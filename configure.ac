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
AC_DEFUN([AC_PROG_STRIP], [AC_CHECK_TOOL(STRIP, strip, :)])
AC_PROG_STRIP
AC_ARG_VAR(STRIP, [Command for discarding symbols from object files])
AC_DEFUN([AC_PROG_AR], [AC_CHECK_TOOL(AR, ar, :)])
AC_PROG_AR
AC_ARG_VAR(AR, [Program that maintains archive files])

################################################################
dnl -- Check for large file support
AC_SYS_LARGEFILE
if test x$ac_cv_sys_file_offset_bits = x64; then
    LFS_FLAGS+="-D_FILE_OFFSET_BITS=64"
fi
if test x$ac_cv_sys_large_files = x1; then
    LFS_FLAGS+="-D_LARGE_FILES"
fi

################################################################
dnl -- Checks for functions.
AC_FUNC_STRERROR_R
if test x$ac_cv_func_strerror_r_char_p = xyes; then
   CXX_STRERROR_FLAG="-DSTRERROR_R_CHAR_P"
fi

################################################################################
dnl -- Prefix is /usr by default, the exec_prefix default is setup later
AC_PREFIX_DEFAULT(/usr)

AC_CHECK_HEADERS([expat.h \
	          iostream \
		  libaio.h \
	          boost/bind.hpp \
		  boost/crc.hpp \
		  boost/lexical_cast.hpp \
		  boost/noncopyable.hpp \
		  boost/optional.hpp \
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
        CXXDEBUG_FLAG+=" -g"
fi

################################################################################
dnl -- Enable gprof
AC_MSG_CHECKING(whether to enable gprof)
AC_ARG_ENABLE(gprof, AC_HELP_STRING([--enable-gprof], [enable gprof]),
              GPROF=$enableval, GPROF=no)
AC_MSG_RESULT($GPROF)

if test x$GPROF = xyes; then
        CXXDEBUG_FLAG+=" -pg"
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
dnl -- Enable development tools
AC_MSG_CHECKING(whether to enable development tools)
AC_ARG_ENABLE(dev-tools,
	      AC_HELP_STRING(--enable-dev-tools, [enable development tools in the makefile]),
	      DEVTOOLS=$enableval, DEVTOOLS=no)
AC_MSG_RESULT($DEVTOOLS)

################################################################################
dnl -- Enable static libstdc++
AC_MSG_CHECKING(whether to statically link libstdc++)
AC_ARG_ENABLE(static_cxx,
              AC_HELP_STRING(--enable-static-cxx, [enable static link of libstdc++]),
              STATIC_CXX=$enableval, STATIC_CXX=no)
AC_MSG_RESULT($STATIC_CXX)

################################################################################
dnl -- Enable static linking.
AC_MSG_CHECKING(whether to statically link)
AC_ARG_ENABLE(static,
              AC_HELP_STRING(--enable-static, [enable static link]),
              STATIC=$enableval, STATIC=no)
AC_MSG_RESULT($STATIC)

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
AC_SUBST(AR)
AC_SUBST(CXXDEBUG_FLAG)
AC_SUBST(CXXOPTIMISE_FLAG)
AC_SUBST(CXX_STRERROR_FLAG)
AC_SUBST(LFS_FLAGS)
AC_SUBST(INSTALL)
AC_SUBST(prefix)
AC_SUBST(RELEASE_DATE)
AC_SUBST(RELEASE_DATE)
AC_SUBST(TESTING)
AC_SUBST(THIN_PROVISIONING_TOOLS_VERSION)
AC_SUBST(STATIC_CXX)
AC_SUBST(DEVTOOLS)
AC_SUBST(STATIC)

################################################################################
dnl -- First and last lines should not contain files to generate in order to
dnl -- keep utility scripts running properly
AC_CONFIG_FILES([
Makefile
contrib/Makefile
unit-tests/Makefile
version.h
src/version.rs
])
AC_OUTPUT
