#!/usr/bin/env bash

# run this script from psmdb source root
NJOBS=""
OPT_TARGETS=""
SCONS_OPTS="--release --opt=on"
ROCKSDB_TARGET="static_lib"
CWD=$(pwd)
TARBALL_SUFFIX=""

# Check if we have a functional getopt(1)
if ! getopt --test
then
  go_out="$(getopt --options=dabuj: \
    --longoptions=debug,asan,dbtest,unittests,jobs: \
    --name="$(basename "$0")" -- "$@")"
  test $? -eq 0 || exit 1
  eval set -- $go_out
fi

for arg 
do
  case "$arg" in
  -- ) shift; break;;
  -d | --debug )
    shift
    SCONS_OPTS="${SCONS_OPTS} --disable-warnings-as-errors --dbg=on"
    ROCKSDB_TARGET="dbg"
    TARBALL_SUFFIX="${TARBALL_SUFFIX}-dbg"
    echo "Debug build selected..."
    ;;  
  -a | --asan )
    shift
    ASAN_OPTIONS="--allocator=system --sanitize=address"
    TARBALL_SUFFIX="${TARBALL_SUFFIX}-asan"
    echo "ASAN build selected..."
    ;;
  -b | --dbtest )
    shift
    OPT_TARGETS="${OPT_TARGETS} dbtest"
    echo "Building of dbtest enabled..."
    ;;
  -u | --unittests )
    shift
    OPT_TARGETS="${OPT_TARGETS} unittests"
    echo "Building of unittests enabled..."
    ;;
  -j | --jobs )
    shift
    NJOBS="$1"
    shift
    ;;
  esac
done

if [ -z "$NJOBS" ]; then
  NJOBS=$(grep -c processor /proc/cpuinfo)
fi

# prepare source
#
CURRENT_DATE=$(date +"%Y%m%d")
REVISION=$(git rev-parse --short HEAD)
REVISION_LONG=$(git rev-parse HEAD)
PSM_VERSION=$(git describe --tags | sed 's/^psmdb-//' | sed 's/^r//' | awk -F '-' '{print $1}')
PSM_RELEASE=$(git describe --tags | sed 's/^psmdb-//' | sed 's/^r//' |awk -F '-' '{print $2}')"@"$CURRENT_DATE
TARBALL_NAME="percona-server-mongodb-${PSM_VERSION}-${PSM_RELEASE}-${REVISION}${TARBALL_SUFFIX}"
# create a proper version.json
echo "{" > version.json
echo "    \"version\": \"${PSM_VERSION}-${PSM_RELEASE}\"," >> version.json
echo "    \"githash\": \"${REVISION_LONG}\"" >> version.json
echo "}" >> version.json
#
#
# build binaries
#
if [ -f /opt/percona-devtoolset/enable ]; then
  source /opt/percona-devtoolset/enable
fi
#
export PATH=/usr/local/go/bin:$PATH
#
if [ -f /etc/debian_version ]; then
  export CC=gcc-5
  export CXX=g++-5
else  
  export CC=$(which gcc)
  export CXX=$(which g++)
fi
#
PSM_TARGETS="mongod mongos mongo mongobridge${OPT_TARGETS}"
ARCH=$(uname -m 2>/dev/null||true)
PSMDIR=$(basename ${CWD})
PSMDIR_ABS=${CWD}
TOOLSDIR="src/mongo/gotools"
TOOLSDIR_ABS="${PSMDIR_ABS}/${TOOLSDIR}"
TOOLS_TAGS="ssl sasl"

# link PSM dir to /tmp to avoid "argument list too long error"
rm -fr /tmp/${PSMDIR}
ln -fs ${PSMDIR_ABS} /tmp/${PSMDIR}
cd /tmp
#
MONGO_TOOLS_TAG="r${PSM_VERSION}"
echo "export PSMDB_TOOLS_COMMIT_HASH=\"${REVISION}\"" > ${TOOLSDIR_ABS}/set_tools_revision.sh
echo "export PSMDB_TOOLS_REVISION=\"${PSM_VERSION}-${PSM_RELEASE}\"" >> ${TOOLSDIR_ABS}/set_tools_revision.sh
chmod +x ${TOOLSDIR_ABS}/set_tools_revision.sh
#
export CFLAGS="${CFLAGS:-} -fno-omit-frame-pointer"
export CXXFLAGS="${CFLAGS}"
export INSTALLDIR=${PSMDIR_ABS}/install
export PORTABLE=1
export USE_SSE=1
#
# static librocksdb.a
pushd ${PSMDIR}/src/third_party/rocksdb
make -j${NJOBS} EXTRA_CFLAGS='-fPIC -DLZ4 -I../lz4-r131 -DSNAPPY -I../snappy-1.1.3 -DHAVE_SSE42' EXTRA_CXXFLAGS='-fPIC -DLZ4 -I../lz4-r131 -DSNAPPY -I../snappy-1.1.3 -DHAVE_SSE42' DISABLE_JEMALLOC=1 ${ROCKSDB_TARGET}
rm -rf ${INSTALLDIR}
mkdir -p ${INSTALLDIR}/include
mkdir -p ${INSTALLDIR}/bin
mkdir -p ${INSTALLDIR}/lib
make install-static INSTALL_PATH=${INSTALLDIR}
popd
#
# Finally build Percona Server for MongoDB with SCons
cd ${PSMDIR_ABS}
buildscripts/scons.py CC=${CC} CXX=${CXX} --ssl ${SCONS_OPTS} -j${NJOBS} --use-sasl-client --wiredtiger --audit --rocksdb --inmemory --hotbackup ${ASAN_OPTIONS} CPPPATH=${INSTALLDIR}/include LIBPATH=${INSTALLDIR}/lib ${PSM_TARGETS}
#
# Build mongo tools
cd ${TOOLSDIR_ABS}
rm -rf vendor/pkg
[[ ${PATH} == *"/usr/local/go/bin"* && -x /usr/local/go/bin/go ]] || export PATH=/usr/local/go/bin:${PATH}
. ./set_gopath.sh
. ./set_tools_revision.sh
mkdir -p bin
for i in bsondump mongostat mongofiles mongoexport mongoimport mongorestore mongodump mongotop mongooplog mongoreplay; do
echo "Building ${i}..."
go build -a -o "bin/$i" -ldflags "-X github.com/mongodb/mongo-tools/common/options.Gitspec=${PSMDB_TOOLS_COMMIT_HASH} -X github.com/mongodb/mongo-tools/common/options.VersionStr=${PSMDB_TOOLS_REVISION}" -tags "${TOOLS_TAGS}" "$i/main/$i.go"
done
# end build tools
# create psmdb tarball
cd ${PSMDIR_ABS}
mkdir -p ${TARBALL_NAME}/bin
cp mongo* ${TARBALL_NAME}/bin
cp ${TOOLSDIR_ABS}/bin/* ${TARBALL_NAME}/bin
tar --owner=0 --group=0 -czf ${TARBALL_NAME}.tar.gz ${TARBALL_NAME}
rm -rf ${TARBALL_NAME}
# move mongo tools to PSM root dir for running tests
mv ${TOOLSDIR_ABS}/bin/* ${PSMDIR_ABS}
