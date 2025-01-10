git stash
git checkout main
git pull
git merge dev/jir
git tag $1
git push
git push origin $1
git checkout dev/jir
git stash pop
