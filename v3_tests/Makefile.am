## This is a automake file, part of Unidata's netCDF package.
# Copyright 2018, see the COPYRIGHT file for more information.

# Put together AM_CPPFLAGS and AM_LDFLAGS.
include $(top_srcdir)/lib_flags.am

BUILT_SOURCES =
EXTRA_DIST =
DISTCLEANFILES =
TESTS =
check_PROGRAMS =

# Following files are V3 specific versions
V3FILES = ${ALLV3BASELINE} ${ALLV3SCRIPTS}

include $(top_srcdir)/nczarr_test/Makefile.files
include $(top_srcdir)/nczarr_test/Makefile.core

AM_TESTS_ENVIRONMENT += export NCZARRFORMAT=3;

EXTRA_DIST += CMakeLists.txt ${V3FILES}

FILES = ${ALLSRC} ${ALLSCRIPTS} ${ALLBASELINE} findplugin.sh

DISTCLEANFILES += ${FILES} ${ALLBUILTSRC} ${ALLBUILTSCRIPTS}

BUILT_SOURCES += ${FILES}

${FILES}:
	echo "ALLSRC=${ALLSRC}"
	echo "FILES=${FILES}"
	for u in $@ ; do cp -f ${top_srcdir}/nczarr_test/$$u $(top_srcdir)/v3_tests ;  done 

# If valgrind is present, add valgrind targets.
@VALGRIND_CHECK_RULES@
