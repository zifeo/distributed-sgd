# Distributed SDG

 This project showcases a distributed implementation of the stochastic gradient descent algorithm. It introduces a synchronous and an asynchronous version. On the one hand, the synchronous version includes a master node which ensures that the computation of the gradient and the update steps of SGD are coordinated among worker nodes. On the other hand, in the asynchronous version the worker nodes perform computation on their own and frequently exchange weight updates among each other. The master only manage the start and end of the full algorithm (e.g. split the work and collect the result).

## Getting started

```shell
cd data
./download.sh
cd ..
sbt
> run
> test
> scalafmt
```

SGD settings can be modified in `src/main/resources/application.conf`.

## Running on Kubernetes

```shell
./build.sh
./run.sh -async
./run.sh -sync
```

SGD settings can be modified in `kube/config-async.conf` and `kube/config-async.conf`.

## Dataset

RCV1 can be downloaded from [here](http://www.ai.mit.edu/projects/jmlr/papers/volume5/lewis04a/lyrl2004_rcv1v2_README.htm).

## References

- RECHT, Benjamin, RE, Christopher, WRIGHT, Stephen, et al. Hogwild: A lock-free approach to parallelizing stochastic gradient descent. In : Advances in neural information processing systems. 2011. p. 693-701.
