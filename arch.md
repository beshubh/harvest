

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


## Todos
 - Tokenization is stupid currently.
 - We have whitespace being a term in the index.
 - We have random binary characters like \䢞<��H����iLC�;��R�+��s�$`�Kƺ��]��m� stored as terms.

 - [x] fix the reader to only read the html documents, skip pdfs or any other type of files.
 - [x] fix jargon binary in the index. 
 - [x] white space should not be a term.

# Query Engine

**Points**
- we will be only doing AND queries, as the target is kinda a search engine like on web.
- Ideal case will be when we are able to provide the index where the term/phrase firs appears in the document.


thinking
INDEX
 term1 -> 3, [docid1, docid2, docid3],
 term1 -> 2, [docid12, docid15], // assuming threshold of 3 for postings max length.
 term2 -> 2, [docid3, docid4]
 term3 -> 3, [docid1, docid4, docid10]

**TODOS** (while thinking):
- [x]we should have a field like bukcet: <int> in `inverted_index` collection, this can maintain the order of docIds, if postings 
of a term overflows the threshold per mongodb document.
- [ ] add skip pointers to the postings_list in `inverted_index`, skip pointers will square_root(|postings_list|) of the term.
  - remember that while calculating the length, we will have to take into account amount of buckets we have per term.
- [x] add positional index to the postings_list in `inverted_index`, positional index will be a list of positions of the term in the document.

Questions (while thinking):
- what if the minimum document frequency is in like 100s of millions?
version 1:
- a naive version would be query the `index` collection with `$in` operator on the terms.
- intersect the resulting posting_lists, starting from the term with lowest document frequecy.
- return the document ids.

version 2:
- we can use skip pointers to skip over the docIds in postings_list while intersecting big posting_lists
- rest is same as version 1.

version 3:
INDEX (positional index)

angels: 2: 〈36,174,252,651〉; 4: 〈12,22,102,432〉; 7: 〈17〉;
fools: 2: 〈1,17,74,222〉; 4: 〈8,78,108,458〉; 7: 〈3,13,23,193〉;
fear: 2: 〈87,704,722,901〉; 4: 〈13,43,113,433〉; 7: 〈18,328,528〉;
in: 2: 〈3,37,76,444,851〉; 4: 〈10,20,110,470,500〉; 7: 〈5,15,25,195〉;
rush: 2: 〈2,66,194,321,702〉; 4: 〈9,69,149,429,569〉; 7: 〈4,14,404〉;
to: 2: 〈47,86,234,999〉; 4: 〈14,24,774,944〉; 7: 〈199,319,599,709〉;
tread: 2: 〈57,94,333〉; 4: 〈15,35,155〉; 7: 〈20,320〉;
where: 2: 〈67,124,393,1001〉; 4: 〈11,41,101,421,431〉; 7: 〈16,36,736〉;

version 4:
query: "angels fear to tread"
- we build a `inverted_index` with position_indices per term.
- we can use the same skip pointers as in version 2.
- we can use position offset value across different terms to find out the documents that contains exact phrase as in the query.

Algorithm
```rust
fn positional_intersect(p1: &PostingList, p2: &PostingList, k: usize) -> Vec<PostingList> {
    let mut answer = Vec::new();
    while p1 != NIL && p2 != NIL {
        if p1.doc_id == p2.doc_id {
            let mut l = Vec::new();
            let pp1 = p1.positions();
            let pp2 = p2.positions();
            while pp1 != NIL {
                while pp2 != NIL {
                    if abs(pp1.position - pp2.position) <= k {
                        l.push(pp2);
                    } else if pp2.postion > pp1.position{
                        // nothing in pp2 can ever be smaller than k distance
                        break;
                    }
                    pp2 = pp2.next();
                }
                while l.not_empty() && abs(l[0] - pp1.position) > k {
                    delete(l[0]);
                }
                for ps in l {
                    answer.push(p1.doc_id, pp1.position, ps);
                }
                pp1 = pp1.next();
            }
            p1 = p1.next();
            p2 = p2.next();
        } 
        else {
            if p1.doc_id < p2.doc_id {
                p1 = p1.next();
            } else {
                p2 = p2.next();
            }
        }
    }
}
```

# TODOS
 - [ ] add a rest api to view the search result documents (results from query engine).
 - [ ] add UI on top of that rest api.
 - [ ] solve for phrase queries using positional intersection.

 