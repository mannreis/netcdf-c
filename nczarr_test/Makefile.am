## This is a automake file, part of Unidata's netCDF package.
# Copyright 2018, see the COPYRIGHT file for more information.

# Put together AM_CPPFLAGS and AM_LDFLAGS.
include $(top_srcdir)/lib_flags.am

include $(top_srcdir)/nczarr_test/Makefile.files
include $(top_srcdir)/nczarr_test/Makefile.core

EXTRA_DIST += CMakeLists.txt Makefile.core Makefile.files

DISTCLEANFILES = findplugin.sh ${BUILT_SOURCES}

# If valgrind is present, add valgrind targets.
@VALGRIND_CHECK_RULES@
