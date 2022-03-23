# Question Answering 
### Parallelizing Entity and Relation linking using Spark

Question Answering in general has three phases:
1. Question Understanding 
2. Entity and Relation Linking 
3. Filtering

./EntityVertexLinking.png

Challenges:
1. Memory constraints
2. Time constraints

Solution:
1. Parallelize the above-mentioned processes using SPARK functions.
2. SPARK functions:
   1. map()
   2. aggregation()
   3. limit()
   4. max()
   5. orderby()
