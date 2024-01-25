## This is a automake file, part of Unidata's netCDF package.
# Copyright 2018, see the COPYRIGHT file for more information.

# Put together AM_CPPFLAGS and AM_LDFLAGS.
include $(top_srcdir)/lib_flags.am

BUILT_SOURCES =
EXTRA_DIST =
DISTCLEANFILES =
TESTS =

include $(top_srcdir)/nczarr_test/Makefile.files

BUILT_SOURCES += ${ALLBUILTSCRIPTS}


# V2 specific files
EXTRA_DIST = CMakeLists.txt Makefile.core Makefile.files ${ALLV2BASELINE} ${ALLV2SCRIPTS}

include $(top_srcdir)/nczarr_test/Makefile.core

if BUILD_UTILITIES
# Test v2 specific tests

# Test fill issues
TESTS += run_nczarr_fill.sh

# Test various corrupted files
TESTS += run_corrupt.sh

# Test interoperability of V2 files
TESTS += run_interop.sh

if ENABLE_EXTERNAL_SERVER_TESTS
TESTS += run_external.sh
endif

endif #BUILD_UTILITIES

DISTCLEANFILES += findplugin.sh ${BUILT_SOURCES}

# If valgrind is present, add valgrind targets.
@VALGRIND_CHECK_RULES@
