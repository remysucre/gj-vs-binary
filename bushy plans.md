Consider the following *spikey* graph, where the middle spikes are longer than the rest: 

![[spikey.png]] 

Suppose the 2 middle spikes are 3-long each, and all other spikes are 2-long, and we want to compute the join: 
$$Q(a_1, a_2, a_3, x, b_3, b_2, b_1) :- R_1(a_1, a_2), R_2(a_2, a_3), R_3(a_3, x), S_3(x, b_3), S_2(b_3, b_2), S_1(b_2, b_1).$$
This join will only return 1 result, i.e. the long spike in the middle. A bushy plan is very efficient: after sorting / hashing each relation, we can start the join from both ends and each join finishes in 1 step. 

Now make $N$ copies of the graph. The same bushy plan is still efficient. The total cost is $4kN\log(kN)+2N\log(N)+5N$: the first 2 terms are for sorting, and the last term for joining. 

No generic join plan can match the run time of the bushy plan above. GJ will require the same cost for sorting, but it takes much longer to join. The idea is, in order to prevent joining on the shorter spikes, GJ must also start from each ends. But one of the ends must be placed after the other in the variable order, and this results in a cartesian product costing $N^2$. Otherwise, starting from either side requires joining the short spikes on the other side, which takes time $kN$. We can make the input a lot more nastier, for example replace the short spikes with some stars and explode the wasted energy. 

We can make the input more demonic in another dimension: chain together multiple spikes to form a caterpillar. To efficiently join on the caterpillar, we would need a truly bushy plan that starts on the hairless parts. 

Possible intuition: long joins is like navigating a maze. If you know the key points in the maze you have to pass through, you'll save a lot of time wandering around. A bushy plan can simultaneously expand around these key points, whereas a single GJ will needlessly create cartesian products. 