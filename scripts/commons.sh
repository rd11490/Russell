
## Recommended preamble for including this script:
#
# . "$(dirname "${BASH_SOURCE[0]}")/common.sh"

set -o nounset -o errexit -o pipefail

# Directory where shell scripts are stored.
export SCRIPTS="$(dirname "${BASH_SOURCE[0]}")"

# Directory where the jar is deployed.
export DIR="$( (cd $SCRIPTS/..; pwd) )"

function _russell_version () {
    # Pull version from pom file. It's in a line like this:
    #   <russell.version>VERSION</russell.version>
    grep russell.version ${DIR}/russell/pom.xml | \
        sed 's/.*>\(.*\)<.*/\1/'
}

RUSSELL_VERSION="$(_russell_version)"

# Russell.jar that is actually launched. Will differ from

export JAR="$DIR/russell/target/russell-$RUSSELL_VERSION-jar-with-dependencies.jar
"