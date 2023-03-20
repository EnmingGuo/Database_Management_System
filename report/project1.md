## Project 1 Report


### 1. Basic information
 - Team #:
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter23-cutecutelovelydoggy
 - Student 1 UCI NetID: enmingg
 - Student 1 Name: Enming Guo
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Internal Record Format
- Show your record format design.

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

### 3. Page Format
- Show your page format design.

![WechatIMG1283.jpeg](..%2Fimages%2FProject1%2FWechatIMG1283.jpeg)


- Explain your slot directory design if applicable.

**The slot block is consist of two unsigned space. The first is used to store the information of "Offset"; the other is used to store the information of "Length".
The function will give the pageNum and slotNum. And I can easily to attain two information of the requested slot.**

### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

**Plan: I used a vector to store each page's vaccant size in order to avoid numerous times of file I/O.
When needing an available-space, I search the vector to find an available-space page.**

**Now: I only check whether the last page is enough now! And the assistant told me to put vacant information in the hidden pages. I will realise it in next projects.**

- How many hidden pages are utilized in your design?

**I only use one hidden page to store `readPageCounter` `writePageCounter` `appendPageCounter` `pageNumber`**

- Show your hidden page(s) format design if applicable

![WechatIMG1284.jpeg](..%2Fimages%2FProject1%2FWechatIMG1284.jpeg)

### 5. Implementation Detail
- Other implementation details goes here.

**I used numerous C programming languages. I use `memcpy` `memset` `malloc` function and the pointer the deal with the bits.** 

### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)