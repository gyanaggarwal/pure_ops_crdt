# pure_ops_crdt
Pure Operation-Based Replicated Data Types

This is an implementation based on a paper "Pure Operation-Based Replicated Data Types" by Carlos Baquero, Paulo Sergio Almeida and Ali Shoker. This paper has been uploaded in docs folder here.

This is a complete implementation of Middleware along with the following CRDTs.

PNCounter - commutative CRDT

GCounter - commutative CRDT

GSet - commutative CRDT

2PSet - commutative CRDT

BoundedCounter - commutative CRDT

MVRegister - non-commutative CRDT

AWSet - non-commutative CRDT

RWSet - non-commutative CRDT

This library implements "at least once delivery" semantics with reliable causal broadcast. It can handle duplicate messages, lost messages and out-of-order messages. This feature converts "at least once delivery" semantics to "exactly once delivery" semantics that is required for operation-based CRDTs.

It also has feature to dynamiclly add a new replica or remove an existing replica. A replica cluster has a cluster_id. A new replica will be added if has the same cluster_id.
