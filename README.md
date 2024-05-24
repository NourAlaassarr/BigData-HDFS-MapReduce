MapReduce Programs for Various Operations
This repository contains MapReduce programs implemented in Hadoop for performing different operations on datasets.

Question 1: Inner and Outer Joins
Consider the following data items representing records in two tables, T1 and T2:

Schema:
T1(A1, A2)
T2(A3, A1)
Data Items:
scss
Copy code
(T1, 1, x)
(T1, 2, x)
(T1, 3, x)
(T1, 4, y)
(T1, 5, y)
(T1, 6, y)
(T1, 7, z)
(T1, 8, z)
(T2, A, 1)
(T2, B, 2)
(T2, C, 3)
Inner Join: Write a MapReduce program to perform an inner join between T1 and T2 where A1 in T1 is a foreign key in T2.

Query Results:

scss
Copy code
(A, 1, x)
(B, 2, x)
(C, 3, x)
Full Outer Join: Write a MapReduce program to perform a full outer join between T1 and T2 where A1 in T1 is a foreign key in T2.

Query Results:

scss
Copy code
(A, 1, x)
(B, 2, x)
(C, 3, x)
(null, 4, y)
(null, 5, y)
(null, 6, y)
(null, 7, z)
(null, 8, z)
Attribute Difference: Write a MapReduce program to find the difference between two attributes, such as A1[T1] – A1[T2]. The result would be [4, 5, 6, 7, 8].

Question 2: Finding Friends of Friends
Consider the following data items representing friendship relationships between persons:

scss
Copy code
(P1, P2)
(P1, P3)
(P3, P4)
(P2, P4)
(P2, P5)
Write a MapReduce program to find the friends of friends for a given person. For example, find the friends of P1's friends. P1's friends would be P2 and P3. Friends of P2 and P3 are P4 and P5.

Question 3: Sorting Key/Value Pairs on HDFS
Given a file containing key/value pairs stored on the Hadoop Distributed File System (HDFS), write a MapReduce program to sort the data in this file using Hadoop's sorting mechanism by the key and store the results back on HDFS.
