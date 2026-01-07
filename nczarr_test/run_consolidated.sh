#!/bin/sh

if test "x$srcdir" = x ; then srcdir=`pwd`; fi
. ../test_common.sh

. "$srcdir/test_nczarr.sh"

set -e

s3isolate "testdir_consolidated"
THISDIR=`pwd`
cd $ISOPATH

serverbin="${execdir}/httpsserver"
PORT=8443
DIRECTORY="folder/subfolder/"

test -f ${serverbin} || {
  echo "http-server not found"
  exit 1
}

echo ""
echo "*** Testing consolidated implementation against simple HTTPS servers "

create_dataset() {
set -x
echo "*** Creating dataset"
mkdir -p folder/subfolder/
cp ${srcdir}/ref_oldformat.zip .
unzip -o ref_oldformat.zip
${NCCOPY} "file://ref_oldformat#mode=nczarr,file" "file://${DIRECTORY}tmp_newformat.file#mode=nczarr,file,consolidated"
${NCDUMP} -n ref_oldformat "file://${DIRECTORY}tmp_newformat.file#mode=zarr,file,consolidated" > ./tmp_newpure.cdl
diff -w ${srcdir}/ref_newformatpure.cdl ./tmp_newpure.cdl
set +x
}

init_server() {
set -x
echo "*** Starting HTTPS server"
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 1 -nodes \
  -subj "/C=XX/ST=None/L=None/O=SelfSigned/CN=localhost" 2>/dev/null

${serverbin} $PORT ./ cert.pem key.pem &
SERVERPID=$!
trap "kill -9 ${SERVERPID}; rm -f cert.pem key.pem" 0

sleep 1
curl --insecure -I https://localhost:${PORT}/${DIRECTORY}tmp_newformat.file/.zmetadata
set +x
}

testcaseconsolidated() {
echo "*** Fetching consolidated dataset from the http server using the ncdump"
set -x
# we need disable SSL verification via the .ncrc
URL="https://localhost:${PORT}/${DIRECTORY}tmp_newformat.file"

# ensure default is to verify hosts
set +e
> ${ISOPATH}/.ncrc
NCZARR_CONSOLIDATED=TRUE ${NCDUMP} "${URL}#mode=zarr,s3"
if [ $? -eq 0 ]; then
  echo "Test failed! Success UNEXPECTED Host is not verified."
  set -e
  exit -1
fi
set -e

echo "[${URL}]HTTP.SSL.VERIFYPEER=0" >> $ISOPATH/.ncrc
echo "[${URL}]HTTP.SSL.VERIFYHOST=0" >> $ISOPATH/.ncrc
NCZARR_CONSOLIDATED=TRUE ${NCDUMP} "${URL}#mode=zarr,s3"
set +x
}

create_dataset
init_server
testcaseconsolidated
