# Is netcdf-4 and/or DAP enabled?
HDF5=1
NCZARR=1
#DAP=1
S3=1
S3I=1
S3TEST=public
CDF5=1
#HDF4=1
FILTERS=1
#XML2=1
CURL=1
THREADSAFE=1
#NONETWORK=1
#NOISY=1

if test "x$CURL" = x ; then unset DAP; fi

export SETX=1

for arg in "$@" ; do
case "$arg" in
vs|VS) VS=1 ;;
linux|nix|l|x) unset VS ;;
nobuild|nb) NOBUILD=1 ;;
notest|nt) NOTEST=1 ;;
*) echo "Must specify env: vs|linux"; exit 1; ;;
esac
done

# Visual Studio

if test "x$VS" != x ; then
NCC="c:/tools/hdf5-1.10.6"
HDF5_DIR="$NCC/cmake/hdf5"
AWSSDK_DIR="/cygdrive/c/tools/aws-sdk-cpp/lib/cmake"
AWSSDK_ROOT_DIR="c:/tools/aws-sdk-cpp"
PTHREADS4W_DIR="$NCC/lib/cmake/"
fi

echo "NOBUILD=${NOBUILD}"
echo "NOTEST=${NOTEST}"

#export NCPATHDEBUG=1

FLAGS=
FLAGS="$FLAGS -DBUILD_SHARED_LIBS=ON"
FLAGS="$FLAGS -DNC_FIND_SHARED_LIBS=ON"
FLAGS="$FLAGS -DCMAKE_IMPORT_LIBRARY_PREFIX=${NCC}"

PREFIX="/tmp/netcdf"

PLUGINDIR="/tmp/plugins"
mkdir -p ${PREFIX}
rm -fr ${PLUGINDIR}
mkdir -p ${PLUGINDIR} 

if test "x$NCZARR" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_NCZARR=true"
else
FLAGS="$FLAGS -DNETCDF_ENABLE_NCZARR=false"
fi

if test "x$THREADSAFE" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_THREADSAFE=true"
if test "x$VS" != x ; then
FLAGS="$FLAGS -DPTHREADS4W_DIR=$PTHREADS4W_DIR"
fi
else
FLAGS="$FLAGS -DNETCDF_ENABLE_THREADSAFE=false"
fi

if test "x$DAP" = x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_DAP=false"
else
FLAGS="$FLAGS -DNETCDF_ENABLE_DAP_REMOTE_TESTS=true"
FLAGS="$FLAGS -DNETCDF_ENABLE_EXTERNAL_SERVER_TESTS=true"
FLAGS="$FLAGS -DNETCDF_ENABLE_DAP_AUTH_TESTS=false"
FLAGS="$FLAGS -DNETCDF_ENABLE_DAP_LONG_TESTS=TRUE"
fi

if test "x$HDF5" = x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_HDF5=false"
else
ignore=1
#FLAGS="$FLAGS -DDEFAULT_API_VERSION:STRING=v110"
#FLAGS="$FLAGS -DHDF5_ROOT=c:/tools/hdf5"
#FLAGS="$FLAGS -DHDF5_ROOT_DIR_HINT=c:/tools/hdf5/cmake/hdf5/hdf5-config.cmake"
FLAGS="$FLAGS -DHDF5_DIR=$HDF5_DIR"
#hdf5-config.cmake
#FLAGS="$FLAGS -DHDF5_LIBRARIES=${NCC}/lib/hdf5 -DHDF5_HL_LIBRARY=${NCC}/lib/hdf5_hl -DHDF5_INCLUDE_DIR=${NCC}/include"
fi
if test "x$CDF5" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_CDF5=true"
fi
if test "x$HDF4" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_HDF4=true"
fi
if test "x$FILTERS" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_PLUGINS=on"
FLAGS="$FLAGS -DNETCDF_ENABLE_FILTER_TESTING=on"
else
FLAGS="$FLAGS -DNETCDF_ENABLE_PLUGINS=off"
FLAGS="$FLAGS -DNETCDF_ENABLE_FILTER_TESTING=off"
fi
if test "x$XML2" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_LIBXML2=on"
else
FLAGS="$FLAGS -DNETCDF_ENABLE_LIBXML2=off"
fi

if test "x$TESTSERVERS" != x ; then
FLAGS="$FLAGS -DREMOTETESTSERVERS=${TESTSERVERS}"
fi

if test "x$S3" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_S3=ON"
if test "x$S3I" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_S3_INTERNAL=ON"
else   # => AWS
FLAGS="$FLAGS -DAWSSDK_ROOT_DIR=${AWSSDK_ROOT_DIR}"
FLAGS="$FLAGS -DAWSSDK_DIR=${AWSSDK_ROOT_DIR}/lib/cmake/AWSSDK"
#PREFIXPATH="$PREFIXPATH;${AWSSDK_ROOT_DIR}"
fi
else
FLAGS="$FLAGS -DNETCDF_ENABLE_S3=OFF"
fi

case "$S3TEST" in
  on*|ON*|yes*|YES*) FLAGS="$FLAGS -DWITH_S3_TESTING=ON" ;;
  off*|OFF*|no*|NO*) FLAGS="$FLAGS -DWITH_S3_TESTING=OFF" ;;
  public*|PUBLIC*) FLAGS="$FLAGS -DWITH_S3_TESTING=PUBLIC" ;;
esac

if test "x$NONETWORK" != x ; then
FLAGS="$FLAGS -DNETCDF_ENABLE_NETWORK_ACCESS=NO"
else
FLAGS="$FLAGS -DNETCDF_ENABLE_NETWORK_ACCESS=YES"
fi

# Enables
FLAGS="$FLAGS -DCMAKE_PREFIX_PATH=${PREFIXPATH}"
FLAGS="$FLAGS -DNETCDF_ENABLE_LOGGING=true"
if test "x$CURL" != x ; then 
FLAGS="$FLAGS -DNETCDF_ENABLE_BYTERANGE=true"
fi
#FLAGS="$FLAGS -DNETCDF_ENABLE_DOXYGEN=true -DNETCDF_ENABLE_INTERNAL_DOCS=true"
#FLAGS="$FLAGS -DNETCDF_ENABLE_LARGE_FILE_TESTS=true"
#FLAGS="$FLAGS -DNETCDF_ENABLE_EXAMPLES=true"
#FLAGS="$FLAGS -DNETCDF_ENABLE_CONVERSION_WARNINGS=false"
#FLAGS="$FLAGS -DNETCDF_ENABLE_TESTS=false"
#FLAGS="$FLAGS -DNETCDF_ENABLE_DISKLESS=false"
#FLAGS="$FLAGS -DBUILD_UTILITIES=false"

FLAGS="$FLAGS -DCURL_NO_CURL_CMAKE=TRUE"
FLAGS="$FLAGS -DNETCDF_ENABLE_UNIT_TESTS=TRUE"

# Withs
#FLAGS="$FLAGS -DPLUGIN_INSTALL_DIR=${PLUGINDIR}"
#FLAGS="$FLAGS -DPLUGIN_INSTALL_DIR=on"
FLAGS="$FLAGS -DNCPROPERTIES_EXTRA=\"key1=value1|key2=value2\""

FLAGS="$FLAGS -DCMAKE_INSTALL_PREFIX=${PREFIX}"

rm -fr build
mkdir build
cd build

NCWD=`pwd`

if test "x$VS" != x ; then

NCLIB=`cygpath -w ${NCWD}/liblib |tr -d ''`
NCCYGLIB=`cygpath -u ${NCLIB} |tr -d ''`
NCCBIN=`cygpath -u "${NCC}/bin" |tr -d ''`
NCCLIB=`cygpath -u "${NCC}/lib" |tr -d ''`
# If using cygwin then must use /cygdrive
AWSSDKBIN="/cygdrive/c/tools/aws-sdk-cpp/bin"

# Visual Studio
PATH="${NCCBIN}:$PATH:$NCCYGLIB"
if test "x$S3" != x ; then
PATH="$PATH:${AWSSDKBIN}"
fi

LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${AWSSDKBIN}:${NCCYGLIB}:${NCCLIB}:${NCCBIN}"
PATH="${PATH}:${LD_LIBRARY_PATH}"

export PATH
export LD_LIBRARY_PATH

CFG="Release"
CCFG="--config ${CFG}"

#G=
#TR=--trace-expand
VERB="-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON"
VERB="${VERB} -DCMAKE_EXPORT_COMPILE_COMMANDS=TRUE"
#VERB="--verbose"
set -x
cmake ${TR} ${VERB} "$G" $FLAGS ..
if test "x$NOBUILD" = x ; then
echo '>>> Building=YES'
cmake --build . ${CCFG}
#cmake --target ZERO_CHECK --build .
#cmake --target ALL_BUILD --build . 
#cp ./plugins/Release/*.lib ./plugins
#cmake --target install --build .
if test "x$NOTEST" = x ; then
echo '>>> Testing=YES'
#cmake ${TR} ${CCFG} --build . --target RUN_TESTS
#cmake --build . --target RUN_TESTS ${CCFG}
if test "x$NOISY" = x1 ; then
ctest -j 12 -VV --timeout 1000 --output-on-failure
else
ctest -j 12 --timeout 1000
fi
else
echo '>>> Testing=NO'
fi
else
echo '>>> Building=NO'
fi

else # !VS

# GCC
NCLIB="${NCWD}/liblib"
#G="-GUnix Makefiles"
#T="--trace"
VERB="-DCMAKE_VERBOSE_MAKEFILE:BOOL=ON"
CFLAGS="-Wno-sign-conversion -Wno-conversion $CFLAGS"
cmake "${G}" ${T} ${VERB} $FLAGS ..
if test "x$NOBUILD" = x ; then
make all
fi
if test "x$NOTEST" = x ; then
#make test
CTFLAGS="$CTFLAGS -j 12"
CTFLAGS="$CTFLAGS --timeout 1000"
if test "x$NOISY" != x ; then
CTFLAGS="$CTFLAGS --output-on-failure"
if test "x$NOISY" = x2 ; then
CTFLAGS="$CTFLAGS -VV"
fi
fi
ctest $CTFLAGS
fi
fi
exit
