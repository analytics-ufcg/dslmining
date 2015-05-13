**What is nMiners?**

It is a DSL that helps students and professional to build their own recommendation system.

With nMinors you can run independent process in parallel, also it is possible choose how many processes do you run in each job.

For example 

```
  parse_data on dataset then
      produce(user_vectors) then
      produce(similarity_matrix using COOCURRENCE as "coocurrence") then
      multiply("coocurrence" by "user_vector") then
      produce(recommendation) write_on output then execute
```

If you wanna see the logs open the file nMiners.log or nMiners-test.log
