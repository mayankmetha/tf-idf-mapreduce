cat book1.txt | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > b1a.hdfs
cat book2.txt | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > b2a.hdfs
cat b1a.hdfs | python3 mapper2.py | sort -k1,1 | python3 reducer2.py > b1c.hdfs
cat b2a.hdfs | python3 mapper2.py | sort -k1,1 | python3 reducer2.py > b2c.hdfs
sed -i -e 's/noname/book1/g' b1c.hdfs 
sed -i -e 's/noname/book2/g' b2c.hdfs
mv b1c.hdfs-e b1b.hdfs
mv b2c.hdfs-e b2b.hdfs
cat b*c.hdfs | python3 mapper3.py | sort -k1,1 | python3 reducer3.py > out.hdfs