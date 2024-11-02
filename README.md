# Kinm is not Mink

Kinm (pronounced "kim", like the name)

Mink (Mink is not Kubernetes), a Kubernetes Aggregated API Server backed by a database, was a hasty rewrite of Kine that instead of
doing an ETCD shim to support an RDBMS, it was a Kubernetes Aggregated API Server that could be backed by an RDBMS.
The idea was great, but the implementation was poor and ultimately the project was archived when Acorn Labs pivoted away
from creating a Kubernetes product.

Kinm is a new project that continues with the learnings of Mink but no longer focused on pure Kubernetes environments.
Compatibility with Kubernetes is not a goal, but happens to work right now. Instead Kinm is focused on providing a
an efficient and scalable API server that can do basically what k8s API does (CRUD operations on resources with Watch).
We will most likely fork away from a lot of the core k8s libraries because they bring in too much bloat and change too
often.

Kinm also heavily focuses on an efficient Postgres backend trying to fully embrace SQL and keep all state in the database
and not in memory as Kine and Mink do.
