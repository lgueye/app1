RELEASE_VERSION=$1
NEXT_DEV_VERSION=$2
git checkout -b release-$RELEASE_VERSION
mvn versions:set -DnewVersion=$RELEASE_VERSION
git commit -am 'Upgrading poms version $RELEASE_VERSION'
mvn clean deploy --skiptTests
git tag -a $RELEASE_VERSION -m 'Tagging version $RELEASE_VERSION'
git push origin $RELEASE_VERSION
git checkout master
mvn versions:set -DnewVersion=$NEXT_DEV_VERSION
git commit -am 'Starting development version $NEXT_DEV_VERSION'
git push origin master
git branch -D release-$RELEASE_VERSION
