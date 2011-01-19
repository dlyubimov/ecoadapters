. ./branch.sh

git checkout $DEV_BRANCH
git tag -d tag-$REL_VERSION
git branch -D $REL_BRANCH-$REL_VERSION
git push $REMOTE :$REL_BRANCH-$REL_VERSION
git push $REMOTE :tag-$REL_VERSION
git pull 



