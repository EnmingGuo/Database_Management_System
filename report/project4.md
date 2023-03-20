## Project 4 Report


### 1. Basic information
- Team #:
- Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter23-cutecutelovelydoggy
- Student 1 UCI NetID: enmingg
- Student 1 Name: Enming Guo
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

![Index_Catalog.png](..%2Fimages%2FProject4%2FIndex_Catalog.png)

The design is just like the former information storage.

### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

After the `getNextTuple()`, I will push it into the Filter method and then screen whether it is qualified.

### 4. Project
- Describe how your project works.

After fetch the complete tuple, I will fetch the needed attributes. Actually, I `toRecord(const void* oldTuple)` first and easily use the record to fetch the needed attributes and then `deRecord(const void* newRecord)` it, then the method is realised. 


### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)

![BNL.png](..%2Fimages%2FProject4%2FBNL.png)

I just used the method told on the slides.

### 6. Index Nested Loop Join
- Describe how your index nested loop join works.

![INL.png](..%2Fimages%2FProject4%2FINL.png)

After fetching the OuterTable record, I will immediately `setIterator()` for the index of InnerTable. After this outertable tuple fetches all the corresponding innertable tuples, we will change to next outertable tuple.

### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.

I use an `Unit` struct including `float` sum and `int` count. And everytime I will `setSum(Unit & unit)` to initial it.
Acoording to the `op`, I will use different strategies to deal with the `unit`. Finally, I will use the `giveResult()` function to calculate the value.

- Describe how your group-based aggregation works. (If you have implemented this feature)

This will use `std::map<T,Unit> TUnitMap` , this data structure will easily help us to get different values of differnt groupby keys.


### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.

No

- Other implementation details:

I include a class called `MyJoin` and all other join methods can share this class jointly.

### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)