# Comparing and Unifying WCOJ with Binary Join
The goal of this project is to compare [generic join](https://gitlab.com/remywang/blog/-/blob/master/posts/wcoj.md) with traditional binary join. We conjecture that the performance of any *linear* binary join plan can be matched by a plan using only generic joins (with a few optimizations). 

We will focus on queries in the [join order benchmark](https://github.com/gregrahn/join-order-benchmark), and compare against an in-memory DB like [DuckDB](https://duckdb.org) or [SQLite](https://www.sqlite.org/index.html). We can consider Postgres if we can figure out how to benchmark against it fairly. 

**TODO**
- [ ] Column store 
- [ ] Indices 

## First experiments
Before jumping into implementation, we should test out our idea with some experiments. For example, we can manually translate a few queries into GJ and compare the performance. Concretely: 
1. Run some queries from the JOB benchmark and see how many only use linear join.  
2. Pick a couple queries (linear ones first) and manually translate to GJ. For this, consider using the implementation [here](https://github.com/mwillsey/egg-smol/blob/a1acf4dc7b6f65f2dd4bbca38d45d40448eda4d8/src/gj.rs#L159) and [here](https://github.com/mwillsey/qry/blob/11c92ff715d0593c0ca86c2b533b9ef8a249bd2c/src/lib.rs#L302). 
3. Pre-filter the input tables. 
4. See if GJ runs faster than the DuckDB baseline. 

## System architecture
![system.png](system.png)

Because we focus on joins, we need to isolate the time spent in filtering. We break each query into two parts, one contains only the joins and the other only the filters. See [normalize](normalize.md) for an example. We use the filters to create intermediate tables (cached), and run the joins on these intermeditates. 

We first run the joins on the tables in DuckDB and measure run time. DuckDB will produce a join plan for the query which we translate into a plan for GJ. It's important that the binary plan is linear; we may need to instruct DuckDB to only consider linear plans. Using the translated plan, we run GJ and get the run time. 

## Join algorithms
We will implement different algorithms for generic join based on which data structure we use to store the relations: 
1. Hash trie
2. Sorted trie
3. Flat sorted array
4. Segmented array

**Hash trie.** Hash trie is easy to implement, and has good support for column-wise layout. Indexing will also be fast since a lookup is constant time. However it is not very cache friendly due to pervasive random access and pointer chasing. Iterating over a hash table can also be slow when the table is sparse, since it takes time proportional to the *capacity* of a hash table to iterate over it. 

**Sorted trie.** Sorted trie will likely be more cache-friendly. It will still chase pointers, but that overhead may be neglegible. Adapting a column-wise layout may be challenging for sorted tries - DuckDB would convert to row-wise before sorting. Indexing may also carry some overhead since lookup (binary search / gallop) is $O(\log(N))$. 

**Flat sorted array.** Storing all tuples in a flat sorted array may further improve locality and eliminate pointer-chasing. Intersection may be slower, because we now have duplicate values for each column. 

**Segmented array.** This is the same way TACO stores sparse tensors (in CSF). It's pretty much the same as the sorted trie, except that the tuples themselves are stored in a single array, and instead of pointers, each trie node stores offsets to that array. This combines some benefits of the previous two storage formats. 

## Indexing
Generic join incurs a linear-ish (linear for hash tries and $O(n \log (n))$ for sorted tries) overhead to load the input relations into tries. This is fine in most cases, since both merge-sort join and hash join need to scan the entire input relations. However, an index join may touch only a tiny fraction of the indexed relation, so we cannot afford to sort the entire relation for generic join. 

The solution is to also create indices for generic join. An index is simply a partially sorted trie. For example, an index for `R(x,y,z,w)` on `x` is a 2-level trie, where the first level stores `x` in sorted order, and each `x` points to an unsorted set of tuples `(y,z,w)`. During generic join execution, we may directly intersect with the pre-sorted `x` level. For each `x` value in the intersection, we can now sort `(y,z,w)` so that we can intersect with them further down the loop. This lazy sorting saves us time from sorting tuples in regions not returned from the index join on `x`. 

## Tensor algebra and generic join
Since relational algebra is equivalent to tensor algebra, the tensor algebra compiler [TACO](http://tensor-compiler.org) implements an algorithm for sparse tensor algebra that precisely coincides with generic join. We may therefore piggyback on TACO to answer queries. 

The main components to implement are: 

1. Translate a binary join plan to a generic join plan (variable order). 
2. Translate the query and plan to tensor algebra. This requires: 
	1. Translating [SQL to CQ](sql2cq.md) 
	2. Rewriting the CQ accoding to the variable order
	3. Translating the CQ to tensor algebra 
3. Translate tables to tensors 

But before jumping into implementation, we should test out our idea with some experiments. For example, we can manually translate a few queries into tensor algebra and compare the performance. Concretely: 
1. Run some queries from the JOB benchmark and see how many only use linear join.  
2. Pick a couple queries (linear ones first) manually translate to tensor algebra. 
3. Translate the input tables of these queries to tensors. 
4. See if the tensor algebra expression runs faster than the DB baseline. 
5. If it goes well, try out some bushy plans too. 

If we're lucky, we'll see speedup / matching performance in the experiments! But it is *still* not time to start implementing. Just like software development, high-quality research should be done in a **test-driven** way. This means we should first work out how to *evaluate* our prototype, *before* implementing the prototype! And for good science we want the evaluation to be **reproduceable**. The steps to set up evaluation is: 
1. Create a docker environment in which we will run all experiments. This involves downloading the benchmark queries and data, installing baseline DBs like DuckDB and SQLite. Carefully document so that anyone can follow it. 
2. Set up scripts to benchmark queries automatically. Ideally, running `./run-experiment.sh` should produce the **exact figures** we will use in our paper. 
3. Of course we can't get the plots for our prototype yet since it does not exist. Put in some arbitrary line for now. 

Several points in this process requires extra care: 
- The optimizer may occasionally choose a bushy plan, in which case we should consider how to [combine](combine-gj.md) multiple GJ plans. 