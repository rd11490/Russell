#!/bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

java -jar $JAR "$@"