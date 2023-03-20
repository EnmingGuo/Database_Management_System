## Debugger and Valgrind Report

### 1. Basic information
 - Team #:
 - Github Repo Link:https://github.com/UCI-Chenli-teaching/cs222-winter23-cutecutelovelydoggy
 - Student 1 UCI NetID: enmingg
 - Student 1 Name: Enming Guo
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Using a Debugger
- Describe how you use a debugger (gdb, or lldb, or CLion debugger) to debug your code and show screenshots. 
For example, using breakpoints, step in/step out/step over, evaluate expressions, etc. 

**I use the CLion debugger to deal with the debug work.**

#### Breakpoints:

![breakpoint.png](..%2Fimages%2Fbreakpoint.png)

![variables.png](..%2Fimages%2Fvariables.png)
**As you can see all the variables are written in the black space. I can check them.**

#### Step in:

![after_few_step_in.png](..%2Fimages%2Fafter_few_step_in.png)

**If I want to the binary code of the buffer, I can turn to the memory view function.**
![check memory.png](..%2Fimages%2Fcheck%20memory.png)

#### Step Over:

![step over.png](..%2Fimages%2Fstep%20over.png)
**After pressing the button of Stepping over, the buffer will be updated.**
![after_step_over.png](..%2Fimages%2Fafter_step_over.png)

#### Evaluate Expression:

![evaluate expression.png](..%2Fimages%2Fevaluate%20expression.png)
**Actually, I don't use this function in the debugging work, but I know how to call it!**

### 3. Using Valgrind
- Describe how you use Valgrind to detect memory leaks and other problems in your code and show screenshot of the Valgrind report.

#### Set up Linux System

**I have no access to ICS Hub. So I use Multipass (a kind of docker) to finish this work.**

##### Git Clone
![valgrind 1.png](..%2Fimages%2Fvalgrind%201.png)

##### Cmake the Project
**cmake ../**
![valgrind 2.png](..%2Fimages%2Fvalgrind%202.png)

**cmake --build .**
![valgrind 3.png](..%2Fimages%2Fvalgrind%203.png)

##### Ctest
![valgrind 4.png](..%2Fimages%2Fvalgrind%204.png)

##### Valgrind Test
**pfm_test**
![valgrind 5.png](..%2Fimages%2Fvalgrind%205.png)
**rbfm_test**
![valgrind 6.png](..%2Fimages%2Fvalgrind%206.png)
![valgrind 7.png](..%2Fimages%2Fvalgrind%207.png)

##### Log Text
**pfm_test**
![valgrind 10.png](..%2Fimages%2Fvalgrind%2010.png)
![valgrind 11.png](..%2Fimages%2Fvalgrind%2011.png)
**rbfm_test**
![valgrind 9.png](..%2Fimages%2Fvalgrind%209.png)
![valgrind 12.png](..%2Fimages%2Fvalgrind%2012.png)
**Because I have not finished the delete and update function. So I certainly will run into error.**