cat book1.txt | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > b1.hdfs
cat book2.txt | python3 mapper1.py | sort -k1,1 | python3 reducer1.py > b2.hdfs
