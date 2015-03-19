# DSLMining
Example of DSL created using Scala. The DSL allows an user scientist to train a user-based recommender and do recommendations to an user.

## Execution
To run you must use the scripts named activator in the root of this folder:

"./activator run"

## Example
```
val recommender = train on_dataset "data/intro1.csv" a USER_BASED_RECOMMENDER using PEARSON_CORRELATION neighbourhoodSize 10

println(recommender to 2l recommends 10)
```

##Structure folder
### src/dslmining/main/main.scala
Run an example of DSL

### src/dslmining/main/api
Contains the business logic

### src/dslmining/main/dsl
Contains the words that will be visible to the user

