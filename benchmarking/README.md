# Benchmarking

One of the goals of the temporal-yugabyte project is to exploit the performance characteristics of Yugabyte YCQL.  We created a tool [temporal-benchmark](https://github.com/manetu/temporal-benchmark) to help quantify the resulting performance in a way that users may easily compare between persistence options, or to candidate enhancements to the YCQL driver.

## Available Test Cases

We have gathered data from a few different scenarios.

- [Single replica deployments via docker-compose](./test-cases/orbstack/README.md)
- [6 node EKS cluster](./test-cases/eks/README.md)