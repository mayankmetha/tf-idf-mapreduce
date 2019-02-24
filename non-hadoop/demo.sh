#!/bin/bash
echo -ne "\033[1;37m[\033[0m\033[1;31m✘\033[0m\033[1;37m]\033[0m\033[1;33m Executing             \033[0m\033[1;37m:\033[0m\033[1;36m Clean Up\033[0m"
rm -rf phase1 phase2 output
mkdir phase1 phase2 output
echo -ne "\r\033[1;37m[\033[0m\033[1;32m✔\033[0m\033[1;37m]\033[0m\033[1;33m Executed              \033[0m\033[1;37m:\033[0m\033[1;36m Clean Up\033[0m\n"
echo -ne "\033[1;37m[\033[0m\033[1;31m✘\033[0m\033[1;37m]\033[0m\033[1;33m Executing             \033[0m\033[1;37m:\033[0m\033[1;36m Phase 1\033[0m"
for filename in ./dataset/*.txt; do
    fname=$(echo "$filename" | grep -Eo 'rfc[0-9]+\.txt')
    cat $filename | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > ./phase1/$fname
done
echo -ne "\r\033[1;37m[\033[0m\033[1;32m✔\033[0m\033[1;37m]\033[0m\033[1;33m Executed              \033[0m\033[1;37m:\033[0m\033[1;36m Phase 1\033[0m\n"
echo -ne "\033[1;37m[\033[0m\033[1;31m✘\033[0m\033[1;37m]\033[0m\033[1;33m Executing             \033[0m\033[1;37m:\033[0m\033[1;36m Phase 2\033[0m"
for filename in ./dataset/*.txt; do
    fname=$(echo "$filename" | grep -Eo 'rfc[0-9]+\.txt')
    cat ./phase1/$fname | python3 mapper2.py | sort -k1,1 | python3 reducer2.py | sed "s/noname/$fname/g" > ./phase2/$fname
done
echo -ne "\r\033[1;37m[\033[0m\033[1;32m✔\033[0m\033[1;37m]\033[0m\033[1;33m Executed              \033[0m\033[1;37m:\033[0m\033[1;36m Phase 2\033[0m\n"
echo -ne "\033[1;37m[\033[0m\033[1;31m✘\033[0m\033[1;37m]\033[0m\033[1;33m Executing             \033[0m\033[1;37m:\033[0m\033[1;36m Phase 3\033[0m"
cat ./phase2/*.txt | python3 mapper3.py | sort -k1,1 | python3 reducer3.py > ./output/final.txt
echo -ne "\r\033[1;37m[\033[0m\033[1;32m✔\033[0m\033[1;37m]\033[0m\033[1;33m Executed              \033[0m\033[1;37m:\033[0m\033[1;36m Phase 3\033[0m\n"