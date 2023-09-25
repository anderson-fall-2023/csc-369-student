---
jupyter:
  jupytext:
    encoding: '# -*- coding: utf-8 -*-'
    formats: ipynb,md,py
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.8.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region slideshow={"slide_type": "slide"} hideCode=false hidePrompt=false -->
# Chapter 9.1 - Cassandra 

Paul E. Anderson
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Ice Breaker

Bob Dylan or the Beatles?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://cdn.educba.com/academy/wp-content/uploads/2019/04/what-is-Cassandra.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Learning objectives:** Our goal is not to learn everything about Cassandra in this lesson. This chapter is meant to provide an introduction to some of the architecture design decisions and implementation details.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Overview
* Distributed
* Fault tolerant
* Scalable
* Column oriented data store
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Data model and disk storage are inspirted by Google's BigTable**

**The cluster technology is inspired by Amazon Dynamo.**
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Architecture Overview
* Designed with expectations of system/hardware failures
* Peer-to-peer distributed system
* All nodes the same
* Data partitioned among all nodes in the cluster
* Read/write anywhere design
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Gossip protocol
Each node communicates with each other through Gossip protocol
<img src="https://media.springernature.com/lw685/springer-static/image/art%3A10.1186%2F1869-0238-4-14/MediaObjects/13174_2012_Article_12_Fig1_HTML.jpg">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Achictecture Continued
* Commit log is used to capture write activity
* Data also written to an in-memory (memtable) and then to disk once memory structure is full
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Schema
* Row-oriented column structure
* Keyspace is similar to a database in the RDBMS world
* A column family is similar to an RDBMS table but more flexible
* A row in a column family is indexed by its key. 
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://www.researchgate.net/profile/Jose-Pereira-91/publication/265890153/figure/fig1/AS:651569481674752@1532357678012/Example-of-column-and-super-column-families.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://studio3t.com/wp-content/uploads/2017/12/cassandra-column-family-example.png?x13993">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Strengths of Cassandra
* Petabyte scalability
* Linear performance gains through adding nodes
* No single point of failure
* Easy replication
* Flexible schema design
* CQL langauge (like SQL)
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Writes
* Definitions
    * Partitioning key - each table has a partitioning key. Helps determine which node in the cluster should have the data
    * Commit log - transactional log. Append only file and provides durability
    * Memtable - memory cache to store the in memory copy of data. Memtable accumulates writes and provides read data
    * SSTable - final destination of data. Actual files on disk are immutable
    * Compaction - periodic process of merging SSTables into a single SStable. Done to optimize read operations
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Write process
1. Cassandra appends writes to commit log on disk
2. Cassandra stores the data in a memory structure called memtable. The memtable is a write-back cache
3. Memtable stores writes in sorted order until limit is reached and then it is flushed
4. When there is a flush, write to a SSTable. 
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://miro.medium.com/max/700/0*wdhGxT-5a5tL7w-1.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Coordinator
* Client can connect to any node and then that node acts as a proxy for the application.
* This proxy node is called the coordinator

<img src="https://miro.medium.com/max/700/0*UJwKzBurhLTzJ3sL.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Partitioner
Each node in a Cassandra cluster (Cassandra ring) is assigned a range of tokens. Example:
<img src="https://miro.medium.com/max/700/0*wCWBHLmZHgZm68-R.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Cassandra distributes data across the cluster using consistent hashing

What is the difference between hashing and consistent hashing?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Answer: When hash table is resized only a subset of the keys need to be remapped.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Once the partitioner applies the hash function to the partition key and gets the token, it knows exactly which node is going to handle the request. Example on next slide:
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://miro.medium.com/max/700/0*iSDYBJ2Gvvcio06n.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Eventual Consistency
* Implies that all updates reach all replicas eventually
* Divergent versions of the same data exist temporarily
* Why does this happen in cassandra and how do we control it?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
When you write to a table in Cassandra you specify the write consistency level. 

This is the number of replica nodes that have to acknowledge the coordinator that update/insert was successful

Once enough of these return, then the client can continue to work even though all the replicas might not have acknowledged the write
<!-- #endregion -->
