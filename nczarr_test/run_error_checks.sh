#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

set -e

. "$srcdir/test_nczarr.sh"

s3isolate "testdir_error_checks"
THISDIR=`pwd`
cd $ISOPATH

testfiltererror() {
    # Mess with the plugin
    cp -r ${srcdir}/ref_data.zarr .
    codec='faultycodecname'
    sed -i 's/blosc/'${codec}'/g' ref_data.zarr/data/.zarray
    ${NCDUMP} -v data -L0  "file://ref_data.zarr#mode=zarr" \
        2>&1 | grep -q "Variable 'data' has unsupported codec: (${codec})"
}


testfiltererror
