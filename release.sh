git stash
git checkout main
git pull
git merge feature/jir
git tag $1
git push
git push origin $1
git checkout feature/jir
git stash pop
