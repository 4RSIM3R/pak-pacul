You are my professor, please give brutally honest review per-point and solution if needed, i plan to submit this to Q1 journal
or publish it as article on IEEE, ACM, VLDB, etc.., so here is the detail : 

So i want to build my own database for my undergraduate thesis (skripsi), i plan to mimick what sqlite do, their standart, etc
but i use b+ tree instead of b tree based like sqlite does, why choose b+ tree, because it want to leverage the sibling 
pointer so it will have better range query an another advantage b+ tree rather than b tree have. And then, i want to introduce
parallel scanning, while sqlite on the paper "SQLite past, present and future" mentioned doesnot have that feature, so i 
assume with adding option parallel scanning it will can boost the scanning type query such as SELECT and JOIN, etc..,
i also want to introduce usage of Hash Join instead of Nested Loop join based that sqlite use. that's is the big picture of
what  i want to build.

I already had experimenting, but i think here several key of improvement from last previous experiment : 

- I fail to achieve good performance both on SELECT or INSERT because i just write / read the entire page during a scan
so, key improvement i want to use is to introduce slotted array approach, where instead of feeding a scanner / worker
entire Page that caming from PAGE_ID * PAGE_SIZE, i just will feed it with Array of slot location where array is located,
also for the insertion, instead of read it, modify it on memory then write it back on disk on PAGE_SIZE, i rather just insert
a row on designated location, in assumption it will boost insertion process performance.
- For delete, i forgot to design a free_list that will store where i can store the free space that i can re-use, honestly 
i dont know how sqlite handle this one, assume we not using that MVCC, WAL thing, since our main goal is focusing on develop
that basic database, emebedded but adding parallel scanning to that one
- And it comes to how i can parallel the b+ tree leaf page, this is quite challengen for me, honestly, i attempt several try 
and here is the journey of that : 

1. My assumption was collecting all leaf page id, and distribute it to N-number of worker, ended up it make performance 
degrade a lot, because we need to collect it (b+ tree leaf is linked list), then we just store it an array, and we write it 
again to disk, 2 work for just one goal, very not feasible. here is my page struct looks likem assume keys are smiliar
ROW_ID in sqlite, in this case it will be just an auto incerement or sequence of rows.

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub page_id: u64,
    pub is_leaf: bool,
    pub parent_page_id: Option<u64>,
    pub keys: Vec<u64>,
    pub values: Vec<Row>,               // Only for leaf nodes
    pub child_page_ids: Vec<u64>,       // Page IDs for children (internal nodes)
    pub next_leaf_page_id: Option<u64>, // Leaf node linking
    pub is_dirty: bool,                 // Track if node needs to be written to disk
}
```

You see, i dont have that slotted array to store which row is positioned, so i must read a whol page (4KB) to just get 
`next_leaf_page_id`, so maybe in the future we can add that slotted approach and then have a function give PAGE_ID
will return array of slot that hold row position, so instead feed it 4KB worker can focus read where row is located and 
process it, and my assumption, we can also add somehting close to row-level, like row level locking, etc

2. My second attempt was to add `leaf_page_registry` that will store all `leaf_page_id` on designated space, yes, it is boost
the performance because we dont need to traverse all the leafs, but it will add more step when insert and delete, not feasible
also

3. Then become to talk about parallel it self, first attempt i just divide it N-number of pages / N-number of worker, the 
result is still not satisfying, collecting result become the culprit, so i decide to make it async, and yaps, it is queit
satisfy but i still cannot stand with the result

so my plan will be for scanning :

- Keep using that traversing method, while i read another production grade database also doing that, but we use slotted 
array approach, every scan will initiated with return slotted array from a page, then feed it into a worker, so reading 
position from Page must very effective, my previous attempt will be like just using `from_bytes` and `to_bytes` very very
un-effective manner.
- When we decide to use sequential scanning we just read that slotted array from page, process it, then go to another page
- But when we want to parallel it, same, we will read slotted array from page, then feed it into a worker stealing queueu
with N-number designated worker thread, then we keep to use that async result collecting approach
- With assumption that worker stealing can prevent thread from starvation problem, make it very effective to distribute
task
- Using some prefetch / read ahead or any cache related techniques to boos the performance

and then my plan for join operation : 

- will using hash join, from now on the spill function we not develop that one, just to show how much it will effect
on JOIN operation especially from SSB benchmark workload

and then for insert operation : 

- Using that slotted array approach to boost insert process
- Handling page overflow, in example like a long TEXT or STRING inserted, we can just spin a new page linked that one

and another for that, follow what sqlite already have.