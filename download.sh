#!/usr/bin/env bash

curl http://www.ai.mit.edu/projects/jmlr/papers/volume5/lewis04a/a13-vector-files/lyrl2004_vectors_test_pt0.dat.gz > lyrl2004_vectors_test_pt0.dat.gz
curl http://www.ai.mit.edu/projects/jmlr/papers/volume5/lewis04a/a13-vector-files/lyrl2004_vectors_test_pt1.dat.gz > lyrl2004_vectors_test_pt1.dat.gz
curl http://www.ai.mit.edu/projects/jmlr/papers/volume5/lewis04a/a13-vector-files/lyrl2004_vectors_test_pt2.dat.gz > lyrl2004_vectors_test_pt2.dat.gz
curl http://www.ai.mit.edu/projects/jmlr/papers/volume5/lewis04a/a13-vector-files/lyrl2004_vectors_test_pt3.dat.gz > lyrl2004_vectors_test_pt3.dat.gz
curl http://www.ai.mit.edu/projects/jmlr/papers/volume5/lewis04a/a13-vector-files/lyrl2004_vectors_train.dat.gz > lyrl2004_vectors_train.dat.gz
curl http://www.ai.mit.edu/projects/jmlr/papers/volume5/lewis04a/a08-topic-qrels/rcv1-v2.topics.qrels.gz > rcv1-v2.topics.qrels.gz

gunzip lyrl2004_vectors_*
gunzip rcv1-v2.topics.qrels.gz

mkdir -p data
mv lyrl2004_vectors_* rcv1-v2.topics.qrels data
