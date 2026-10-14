package river

import "github.com/riverqueue/river/internal/joblease"

// JobLeaseConfig enables renewable leases when assigned to Config.JobLease.
// An empty configuration uses a 20-second heartbeat and a 120-second lease.
// All runtimes sharing the schema must understand leases before enabling them.
// Work must honor context cancellation; River cannot forcibly stop goroutines.
type JobLeaseConfig = joblease.Config
