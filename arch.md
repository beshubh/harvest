

# Indexer

## SPIMI

- SPIMI is a single pass in memory indexing
- iterate over the token stream
- until the memory is full keep accumulating.
- once memory is full, flush the buffer to the db.
- do this till token stream ends.
- merge all the blocks once token stream ends.

## MongoDB as disk.
- We will be using mongo db as disk storage for indexing.
- When memory is full, we will take the in memory sorted hashmap.
- Insert the sorted hasmap to mongodb.
- Aglorithm to do so.
    - For each block we will have a separate mongo collection. (Using collection as a file).
    - For each key in this hashmap.
    - Create the docs in mongodb, based on the how many elements are there in the list of docids for this key.
    - we will have a part size of 1 million.
    - So if a term has 10 million docs, we will have 10 mongo db documents for this term in this mongo collection.
- So if total terms are 10 million with 2million average docs per term, we will have 20 million mongo documents in this collection.
- We don't really need to the sort the terms in dictionary, we can just add index to mongo on terms and sort on that.


- Once all the blocks are written to their respective collections.

## Merge 
- Open a buffer for each collection that stored the block, it should be sorted by term obviously.
- Open a buffer for the merged collection.
- Do merge sort.

## Interfaces

```rust

struct Token(Term, DocID);

struct Block {
    sorted_terms: Vec<Term>
    dictionary: HashMap<Term, Vec<DocId>>
}

struct 
```
