package craft

import (
	"sync"
)

// TimeCommitment is used to advance the leader's commit index. The leader and
// replication goroutines report in newly written entries with Match(), and
// this notifies on commitCh when the commit timestamp has advanced.
type timeCommitment struct {
	// protects matchTimes and commitTime
	sync.Mutex
	// notified when commitTime increases
	commitCh chan struct{}
	// voter ID to timestamps: the server stores up through this log entry
	matchTimes map[ServerID]int64
	// a quorum stores up through this log entry. monotonically increases.
	commitTime int64
	// the first timestamp of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	startTime int64
}

// newTimeCommitment returns an timeCommitment struct that notifies the provided
// channel when log entries have been committed. A new timeCommitment struct is
// created each time this server becomes leader for a particular term.
// 'configuration' is the servers in the cluster.
// 'startTime' is the first timestamp created in this term (see
// its description above).
func newTimeCommitment(commitCh chan struct{}, configuration Configuration, startTime int64) *timeCommitment {
	matchTimes := make(map[ServerID]int64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			matchTimes[server.ID] = 0
		}
	}
	return &timeCommitment{
		commitCh:   commitCh,
		matchTimes: matchTimes,
		commitTime: 0,
		startTime:  startTime,
	}
}

// Called when a new cluster membership configuration is created: it will be
// used to determine timeCommitment from now on. 'configuration' is the servers in
// the cluster.
func (c *timeCommitment) setConfiguration(configuration Configuration) {
	c.Lock()
	defer c.Unlock()
	oldmatchTimes := c.matchTimes
	c.matchTimes = make(map[ServerID]int64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			c.matchTimes[server.ID] = oldmatchTimes[server.ID] // defaults to 0
		}
	}
	c.recalculate()
}

// Called by leader after commitCh is notified
func (c *timeCommitment) getCommitTime() int64 {
	c.Lock()
	defer c.Unlock()
	return c.commitTime
}

// Match is called once a server completes writing entries to disk: either the
// leader has written the new entry or a follower has replied to an
// AppendEntries RPC. The given server's disk agrees with this server's log up
// through the given index.
func (c *timeCommitment) match(server ServerID, matchTime int64) {
	c.Lock()
	defer c.Unlock()
	if prev, hasVote := c.matchTimes[server]; hasVote && matchTime > prev {
		c.matchTimes[server] = matchTime
		c.recalculate()
	}
}

// Internal helper to calculate new commitTime from matchTimes.
// Must be called with lock held.
func (c *timeCommitment) recalculate() {
	if len(c.matchTimes) == 0 {
		return
	}

	// find smallest
	var quorumMatchTime int64
	for _, t := range c.matchTimes {
		if quorumMatchTime == 0 || t < quorumMatchTime {
			quorumMatchTime = t
		}
	}

	if quorumMatchTime > c.commitTime && quorumMatchTime >= c.startTime {
		c.commitTime = quorumMatchTime
		asyncNotifyCh(c.commitCh)
	}
}
