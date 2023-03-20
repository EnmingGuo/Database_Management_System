## Project 2 Report


### 1. Basic information
 - Team #:
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter23-cutecutelovelydoggy
 - Student 1 UCI NetID:enmingg
 - Student 1 Name:Enming Guo
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

![WechatIMG22.jpeg](..%2Fimages%2FProject2%2FWechatIMG22.jpeg)

**I added a new column in the Tables, which is `authority`. This is to show that whether this table can be deleted.**

**For the Column table, it is designed as the Chapter 4 slide.**


### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design. 

**No change!**

  ![record_format_design.jpeg](..%2Fimages%2FProject1%2Frecord_format_design.jpeg)
  **The first unsigned block is used to store the number of record items.
  The following n unsigned blocks are used to store each the end index of each item.
  The other blocks are used to store each item.**


- Describe how you store a null field.


![WechatIMG1282.jpeg](..%2Fimages%2FProject1%2FWechatIMG1282.jpeg)
**I just skip every null item and make its pointer block has no increment in comparision to the previous one.**

- Describe how you store a VarChar field.

**Through my method, I can easily get the size of each VarChar field. And the memory to store Varchar is accorded to its real size.**


- Describe how your record design satisfies O(1) field access.

**I can calculate the start index and end index through O(1) algorithm. The start index is the value in the (1+i) × sizeof(unsigned) address.
And the end index is the value of the (1+i+1) × sizeof(unsigned) address.**


### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

![WechatIMG23.jpeg](..%2Fimages%2FProject2%2FWechatIMG23.jpeg)

**I add a new area called `vacant_slot_number`, this is used to show the number of vacant slots. If there is none, I can avoid a traverse of the whole slots in this page to find a vacant slot.**

- Explain your slot directory design if applicable.

**The slot directories are stored at the end of the whole pages. And I use 8 bytes to store the `Offset` and `Length`. Although 4 bytes are enough, through this I avoid great adaptaion in future DBSM implement.**


### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

**I only use one hidden page.**

- Show your hidden page(s) format design if applicable

![WechatIMG24.jpeg](..%2Fimages%2FProject2%2FWechatIMG24.jpeg)

**I used an additional area for the file of Table. This field is called `Current_Table_Counter` . This is used to give id for every table.**

### 6. Describe the following operation logic.
- Delete a record

**Through the given rid information, I can use O(1) method to access the record. And I will remove the record and move other records to occupy the former record area.**

**However, I won't remove the slot directory. And I will use a signal to show that this slot is empty. When next insertion comes. I will firstly use this slot.**

- Update a record

**This is really tough work. I will first delete this record. And at this time the slot is empty, I will insert the new record into this slot. And the new Offset and will be stored.**

**However, chance that the current page doesn't have enough space, so I will insert the new record into another page. After that I will put the new RID into the previous slot.**

**Consequently, I will have easy access to the new record. And I will also put a signal into the previous slot to illustrate this new record is stored in another page.**

- Scan on normal records

**This work is not hard. I will try to get new projectattributes(vector). And I used previous realized method to finish this work.** 


- Scan on deleted records

**I will skip the deleted records. Because I have special signs to show this slot store no `Offset` and `Length`.**


- Scan on updated records

**If the corresponding record is stored in another pages, in the slot it will show. And I will get the record from the other page.**

### 7. Implementation Detail
- Other implementation details goes here.

**(1) I add a table(additionTable) to pass the test case.**

**(2) I use almost C language to finish this work.**

### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)