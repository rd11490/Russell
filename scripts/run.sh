#!/bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/commons.sh"

echo $JAR

scala  -J-Xmx16g -cp $JAR "$@"
