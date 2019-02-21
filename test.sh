echo -ne "\033[1;37m[\033[0m\033[1;31m✘\033[0m\033[1;37m]\033[0m\033[1;33m Executing             \033[0m\033[1;37m:\033[0m\033[1;36m Clean Up\033[0m"
rm *.hdfs
echo -ne "\r\033[1;37m[\033[0m\033[1;32m✔\033[0m\033[1;37m]\033[0m\033[1;33m Executed              \033[0m\033[1;37m:\033[0m\033[1;36m Clean Up\033[0m\n"
echo -ne "\033[1;37m[\033[0m\033[1;31m✘\033[0m\033[1;37m]\033[0m\033[1;33m Executing             \033[0m\033[1;37m:\033[0m\033[1;36m Phase 1\033[0m"
cat book1.txt | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > b1a.hdfs
cat book2.txt | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > b2a.hdfs
echo -ne "\r\033[1;37m[\033[0m\033[1;32m✔\033[0m\033[1;37m]\033[0m\033[1;33m Executed              \033[0m\033[1;37m:\033[0m\033[1;36m Phase 1\033[0m\n"
echo -ne "\033[1;37m[\033[0m\033[1;31m✘\033[0m\033[1;37m]\033[0m\033[1;33m Executing             \033[0m\033[1;37m:\033[0m\033[1;36m Phase 2\033[0m"
cat b1a.hdfs | python3 mapper2.py | sort -k1,1 | python3 reducer2.py > b1c.hdfs
cat b2a.hdfs | python3 mapper2.py | sort -k1,1 | python3 reducer2.py > b2c.hdfs
sed -i -e 's/noname/book1/g' b1c.hdfs 
sed -i -e 's/noname/book2/g' b2c.hdfs
mv b1c.hdfs-e b1b.hdfs
mv b2c.hdfs-e b2b.hdfs
echo -ne "\r\033[1;37m[\033[0m\033[1;32m✔\033[0m\033[1;37m]\033[0m\033[1;33m Executed              \033[0m\033[1;37m:\033[0m\033[1;36m Phase 2\033[0m\n"
echo -ne "\033[1;37m[\033[0m\033[1;31m✘\033[0m\033[1;37m]\033[0m\033[1;33m Executing             \033[0m\033[1;37m:\033[0m\033[1;36m Phase 3\033[0m"
cat b*c.hdfs | python3 mapper3.py | sort -k1,1 | python3 reducer3.py > out.hdfs
echo -ne "\r\033[1;37m[\033[0m\033[1;32m✔\033[0m\033[1;37m]\033[0m\033[1;33m Executed              \033[0m\033[1;37m:\033[0m\033[1;36m Phase 3\033[0m\n"