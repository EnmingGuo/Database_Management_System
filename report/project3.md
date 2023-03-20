## Project 3 Report


### 1. Basic information
 - Team #:
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter23-cutecutelovelydoggy
 - Student 1 UCI NetID: enmingg
 - Student 1 Name: Enming Guo
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 

  (1) Hidden Page

     ![Hidden Page.png](..%2Fimages%2FProject3%2FHidden%20Page.png)


  **I don't have other Meta-data Page !**


### 3. Index Entry Format
- Show your index entry design (structure). 

  - entries on internal nodes:  
  ![Non-leaf Entry.png](..%2Fimages%2FProject3%2FNon-leaf%20Entry.png)
  - entries on leaf nodes:
![Leaf Entry.png](..%2Fimages%2FProject3%2FLeaf%20Entry.png)


### 4. Page Format
- Show your internal-page (non-leaf node) design.

![Non-Leaf Page.png](..%2Fimages%2FProject3%2FNon-Leaf%20Page.png)


- Show your leaf-page (leaf node) design.

![Leaf Page.png](..%2Fimages%2FProject3%2FLeaf%20Page.png)

### 5. Describe the following operation logic.
- Split

**I will depend on the slotNum to split the half inside the previous page and the other half inside the later page.**

- Rotation (if applicable)

**When the split operation happens, I will save the first entry in the new page inside the Childentry. And when it comes to higher level, I will put the childentry inside the higher nodes.**

- Merge/non-lazy deletion (if applicable)

**I realise the non-lazy, but I will delete the empty index entry. When deletion happens in leaf-index entry and nonleaf-index entry, I will compact the slot and the record to save memory.**

- Duplicate key span in a page

**Because I save the key and rid inside the leaf entry and non-leaf entry, in the leaf pages, record with same key with different rids will sort in a line. And I can easily fetch them all.**

- Duplicate key span multiple pages (if applicable)

**This is similar method with the upper question.** 

### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.


  **No!!!**

- Other implementation details:

**I realised the double-direction link in the leaf Page!**

### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
