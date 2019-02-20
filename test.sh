cat book1.txt | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > b1a.hdfs
cat book2.txt | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > b2a.hdfs
cat b1a.hdfs | python3 mapper2.py | sort -k1,1 | python3 reducer2.py > b1b.hdfs
cat b2a.hdfs | python3 mapper2.py | sort -k1,1 | python3 reducer2.py > b2b.hdfs
