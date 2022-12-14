package craft

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	metrics "github.com/armon/go-metrics"
)

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

var (
	// ErrLogNotFound indicates a given log entry is not available.
	ErrLogNotFound = errors.New("log not found")

	// ErrPipelineReplicationNotSupported can be returned by the transport to
	// signal that pipeline replication is not supported in general, and that
	// no error message should be produced.
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
)

// followerReplication is in charge of sending snapshots and log entries from
// this leader during this particular term to a remote follower.
type followerReplication struct {
	// peer contains the network address and ID of the remote follower.
	peer Server

	// commitment tracks the entries acknowledged by followers so that the
	// leader's commit index can advance. It is updated on successful
	// AppendEntries responses.
	commitment *commitment

	// stopCh is notified/closed when this leader steps down or the follower is
	// removed from the cluster. In the follower removed case, it carries a log
	// index; replication should be attempted with a best effort up through that
	// index, before exiting.
	stopCh chan uint64
	// triggerCh is notified every time new entries are appended to the log.
	triggerCh chan struct{}

	// currentTerm is the term of this leader, to be included in AppendEntries
	// requests.
	currentTerm uint64
	// nextIndex is the index of the next log entry to send to the follower,
	// which may fall past the end of the log.
	nextIndex uint64

	// lastContact is updated to the current time whenever any response is
	// received from the follower (successful or not). This is used to check
	// whether the leader should step down (Raft.checkLeaderLease()).
	lastContact time.Time
	// lastContactLock protects 'lastContact'.
	lastContactLock sync.RWMutex

	// failures counts the number of failed RPCs since the last success, which is
	// used to apply backoff.
	failures uint64

	// notifyCh is notified to send out a heartbeat, which is used to check that
	// this server is still leader.
	notifyCh chan struct{}
	// notify is a map of futures to be resolved upon receipt of an
	// acknowledgement, then cleared from this map.
	notify map[*verifyFuture]struct{}
	// notifyLock protects 'notify'.
	notifyLock sync.Mutex

	// stepDown is used to indicate to the leader that we
	// should step down based on information from a follower.
	stepDown chan struct{}

	// allowPipeline is used to determine when to pipeline the AppendEntries RPCs.
	// It is private to this replication goroutine.
	allowPipeline bool

	// craft
	// timeCommitment
	timeCommitment *timeCommitment
}

// notifyAll is used to notify all the waiting verify futures
// if the follower believes we are still the leader.
func (s *followerReplication) notifyAll(leader bool) {
	// Clear the waiting notifies minimizing lock time
	s.notifyLock.Lock()
	n := s.notify
	s.notify = make(map[*verifyFuture]struct{})
	s.notifyLock.Unlock()

	// Submit our votes
	for v, _ := range n {
		v.vote(leader)
	}
}

// cleanNotify is used to delete notify, .
func (s *followerReplication) cleanNotify(v *verifyFuture) {
	s.notifyLock.Lock()
	delete(s.notify, v)
	s.notifyLock.Unlock()
}

// LastContact returns the time of last contact.
func (s *followerReplication) LastContact() time.Time {
	s.lastContactLock.RLock()
	last := s.lastContact
	s.lastContactLock.RUnlock()
	return last
}

// setLastContact sets the last contact to the current time.
func (s *followerReplication) setLastContact() {
	s.lastContactLock.Lock()
	s.lastContact = time.Now()
	s.lastContactLock.Unlock()
}

// replicate is a long running routine that replicates log entries to a single
// follower.
func (r *Raft) replicate(s *followerReplication) {
	// Start an async heartbeating routing
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	r.goFunc(func() { r.heartbeat(s, stopHeartbeat) })

RPC:
	shouldStop := false
	for !shouldStop {
		select {
		case maxIndex := <-s.stopCh:
			// Make a best effort to replicate up to this index
			if maxIndex > 0 {
				r.replicateTo(s, maxIndex)
			}
			return
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		case <-randomTimeout(r.conf.CommitTimeout): // TODO: what is this?
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		}

		// If things looks healthy, switch to pipeline mode
		if !shouldStop && s.allowPipeline {
			goto PIPELINE
		}
	}
	return

PIPELINE:
	// Disable until re-enabled
	s.allowPipeline = false

	// Replicates using a pipeline for high performance. This method
	// is not able to gracefully recover from errors, and so we fall back
	// to standard mode on failure.
	if err := r.pipelineReplicate(s); err != nil {
		if err != ErrPipelineReplicationNotSupported {
			r.logger.Printf("[ERR] raft: Failed to start pipeline replication to %v: %s", s.peer, err)
		}
	}
	goto RPC
}

// replicateTo is a helper to replicate(), used to replicate the logs up to a
// given last index.
// If the follower log is behind, we take care to bring them up to date.
func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) (shouldStop bool) {
	// Create the base request
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
	var start time.Time
START:
	// Prevent an excessive retry rate on errors
	if s.failures > 0 {
		select {
		case <-time.After(backoff(failureWait, s.failures, maxFailureScale)):
		case <-r.shutdownCh:
		}
	}

	// Setup the request
	if err := r.setupAppendEntries(s, &req, s.nextIndex, lastIndex); err == ErrLogNotFound {
		goto SEND_SNAP
	} else if err != nil {
		return
	}

	// Make the RPC call
	start = time.Now()
	if err := r.trans.AppendEntries(s.peer.ID, s.peer.Address, &req, &resp); err != nil {
		// craft
		// avoid too much logging
		if s.failures < 2 {
			r.logger.Printf("[ERR] raft: Failed to AppendEntries to %v: %v", s.peer, err)
		}
		s.failures++
		return
	}
	appendStats(string(s.peer.ID), start, float32(len(req.Entries)))

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true
	}

	// Update the last contact
	s.setLastContact()

	// Update s based on success
	if resp.Success {
		// Update our replication state
		updateLastAppended(s, &req)

		// craft
		updateLastAppendedTime(s, &resp)
		r.handleFastUpdate(s, &req, &resp)

		// Clear any failures, allow pipelining
		s.failures = 0
		s.allowPipeline = true
	} else {
		s.nextIndex = max(min(s.nextIndex-1, resp.LastLog+1), 1)
		if resp.NoRetryBackoff {
			s.failures = 0
		} else {
			s.failures++
		}
		r.logger.Printf("[WARN] raft: AppendEntries to %v rejected, sending older logs (next: %d)", s.peer, s.nextIndex)
	}

CHECK_MORE:
	// Poll the stop channel here in case we are looping and have been asked
	// to stop, or have stepped down as leader. Even for the best effort case
	// where we are asked to replicate to a given index and then shutdown,
	// it's better to not loop in here to send lots of entries to a straggler
	// that's leaving the cluster anyways.
	select {
	case <-s.stopCh:
		return true
	default:
	}

	// Check if there are more logs to replicate
	if s.nextIndex <= lastIndex {
		goto START
	}
	return

	// SEND_SNAP is used when we fail to get a log, usually because the follower
	// is too far behind, and we must ship a snapshot down instead
SEND_SNAP:
	if stop, err := r.sendLatestSnapshot(s); stop {
		return true
	} else if err != nil {
		r.logger.Printf("[ERR] raft: Failed to send snapshot to %v: %v", s.peer, err)
		return
	}

	// Check if there is more to replicate
	goto CHECK_MORE
}

// sendLatestSnapshot is used to send the latest snapshot we have
// down to our follower.
func (r *Raft) sendLatestSnapshot(s *followerReplication) (bool, error) {
	// Get the snapshots
	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to list snapshots: %v", err)
		return false, err
	}

	// Check we have at least a single snapshot
	if len(snapshots) == 0 {
		return false, fmt.Errorf("no snapshots found")
	}

	// Open the most recent snapshot
	snapID := snapshots[0].ID
	meta, snapshot, err := r.snapshots.Open(snapID)
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to open snapshot %v: %v", snapID, err)
		return false, err
	}
	defer snapshot.Close()

	// Setup the request
	req := InstallSnapshotRequest{
		RPCHeader:          r.getRPCHeader(),
		SnapshotVersion:    meta.Version,
		Term:               s.currentTerm,
		Leader:             r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      encodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}

	// Make the call
	start := time.Now()
	var resp InstallSnapshotResponse
	if err := r.trans.InstallSnapshot(s.peer.ID, s.peer.Address, &req, &resp, snapshot); err != nil {
		r.logger.Printf("[ERR] raft: Failed to install snapshot %v: %v", snapID, err)
		s.failures++
		return false, err
	}
	metrics.MeasureSince([]string{"raft", "replication", "installSnapshot", string(s.peer.ID)}, start)

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true, nil
	}

	// Update the last contact
	s.setLastContact()

	// Check for success
	if resp.Success {
		// Update the indexes
		s.nextIndex = meta.Index + 1
		s.commitment.match(s.peer.ID, meta.Index)

		// Clear any failures
		s.failures = 0

		// Notify we are still leader
		s.notifyAll(true)
	} else {
		s.failures++
		r.logger.Printf("[WARN] raft: InstallSnapshot to %v rejected", s.peer)
	}
	return false, nil
}

// heartbeat is used to periodically invoke AppendEntries on a peer
// to ensure they don't time out. This is done async of replicate(),
// since that routine could potentially be blocked on disk IO.
func (r *Raft) heartbeat(s *followerReplication, stopCh chan struct{}) {
	var failures uint64
	req := AppendEntriesRequest{
		RPCHeader: r.getRPCHeader(),
		Term:      s.currentTerm,
		Leader:    r.trans.EncodePeer(r.localID, r.localAddr),
	}
	var resp AppendEntriesResponse
	for {
		// Wait for the next heartbeat interval or forced notify
		select {
		case <-s.notifyCh:
		case <-randomTimeout(r.conf.HeartbeatTimeout / 10):
		case <-stopCh:
			return
		}

		start := time.Now()
		if err := r.trans.AppendEntries(s.peer.ID, s.peer.Address, &req, &resp); err != nil {
			// craft
			if failures < 2 {
				r.logger.Printf("[ERR] raft: Failed to heartbeat to %v: %v", s.peer.Address, err)
			}
			failures++
			select {
			case <-time.After(backoff(failureWait, failures, maxFailureScale)):
			case <-stopCh:
			}
		} else {
			s.setLastContact()
			failures = 0
			metrics.MeasureSince([]string{"raft", "replication", "heartbeat", string(s.peer.ID)}, start)
			s.notifyAll(resp.Success)

			// craft
			if s.peer.Priority > r.priority {
				r.logger.Printf("[DEBUG] raft: peer %v has higher priority than mine (%v, %v), prepare to step down\n",
					s.peer.Address, s.peer.Priority, r.priority)
				r.isResigning = true
			}
			lastIndex := r.getLastIndex()
			// craft
			// step down if a follwer has higher priority and its log is up-to-date
			if r.isResigning && s.peer.Priority > r.priority && s.nextIndex >= lastIndex {
				r.stepDown(s)
			}
		}

		// craft
		if r.craftConfigured && r.merger.NeedSync() {
			r.addSyncEntry()
		}
	}
}

// pipelineReplicate is used when we have synchronized our state with the follower,
// and want to switch to a higher performance pipeline mode of replication.
// We only pipeline AppendEntries commands, and if we ever hit an error, we fall
// back to the standard replication which can handle more complex situations.
func (r *Raft) pipelineReplicate(s *followerReplication) error {
	// Create a new pipeline
	pipeline, err := r.trans.AppendEntriesPipeline(s.peer.ID, s.peer.Address)
	if err != nil {
		return err
	}
	defer pipeline.Close()

	// Log start and stop of pipeline
	r.logger.Printf("[INFO] raft: pipelining replication to peer %v", s.peer)
	defer r.logger.Printf("[INFO] raft: aborting pipeline replication to peer %v", s.peer)

	// Create a shutdown and finish channel
	stopCh := make(chan struct{})
	finishCh := make(chan struct{})

	// Start a dedicated decoder
	r.goFunc(func() { r.pipelineDecode(s, pipeline, stopCh, finishCh) })

	// Start pipeline sends at the last good nextIndex
	nextIndex := s.nextIndex

	shouldStop := false
SEND:
	for !shouldStop {
		select {
		case <-finishCh:
			break SEND
		case maxIndex := <-s.stopCh:
			// Make a best effort to replicate up to this index
			if maxIndex > 0 {
				r.pipelineSend(s, pipeline, &nextIndex, maxIndex)
			}
			break SEND
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		case <-randomTimeout(r.conf.CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		}
	}

	// Stop our decoder, and wait for it to finish
	close(stopCh)
	select {
	case <-finishCh:
	case <-r.shutdownCh:
	}
	return nil
}

// pipelineSend is used to send data over a pipeline. It is a helper to
// pipelineReplicate.
func (r *Raft) pipelineSend(s *followerReplication, p AppendPipeline, nextIdx *uint64, lastIndex uint64) (shouldStop bool) {
	// Create a new append request
	req := new(AppendEntriesRequest)
	if err := r.setupAppendEntries(s, req, *nextIdx, lastIndex); err != nil {
		return true
	}

	// Pipeline the append entries
	if _, err := p.AppendEntries(req, new(AppendEntriesResponse)); err != nil {
		r.logger.Printf("[ERR] raft: Failed to pipeline AppendEntries to %v: %v", s.peer, err)
		return true
	}

	// Increase the next send log to avoid re-sending old logs
	if n := len(req.Entries); n > 0 {
		last := req.Entries[n-1]
		*nextIdx = last.Index + 1
	}
	return false
}

// pipelineDecode is used to decode the responses of pipelined requests.
func (r *Raft) pipelineDecode(s *followerReplication, p AppendPipeline, stopCh, finishCh chan struct{}) {
	defer close(finishCh)
	respCh := p.Consumer()
	for {
		select {
		case ready := <-respCh:
			req, resp := ready.Request(), ready.Response()
			appendStats(string(s.peer.ID), ready.Start(), float32(len(req.Entries)))

			// craft
			// if len(req.Entries) > 0 {
			// 	r.logger.Printf("[DEBUG] raft: group %v index %v receiving response after %v\n",
			// 	r.groupID, req.Entries[0].Index, time.Since(time.Unix(0, req.Entries[0].Timestamp)))
			// }

			// Check for a newer term, stop running
			if resp.Term > req.Term {
				r.handleStaleTerm(s)
				return
			}

			// Update the last contact
			s.setLastContact()

			// Abort pipeline if not successful
			if !resp.Success {
				return
			}

			// Update our replication state
			updateLastAppended(s, req)

			// craft
			updateLastAppendedTime(s, resp)
			r.handleFastUpdate(s, req, resp)
		case <-stopCh:
			return
		}
	}
}

// setupAppendEntries is used to setup an append entries request.
func (r *Raft) setupAppendEntries(s *followerReplication, req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	req.RPCHeader = r.getRPCHeader()
	req.Term = s.currentTerm
	req.Leader = r.trans.EncodePeer(r.localID, r.localAddr)
	req.LeaderCommitIndex = r.getCommitIndex()
	if err := r.setPreviousLog(req, nextIndex); err != nil {
		return err
	}
	if err := r.setNewLogs(req, nextIndex, lastIndex); err != nil {
		return err
	}
	// craft
	if r.nGroups > 1 {
		req.ApplyIndexes = r.merger.GetApplyIndexes()
		// next safe time after replicating entries in this request
		if len(req.Entries) > 0 {
			req.NextSafeTime = r.nextSafeTime(req.Entries[len(req.Entries)-1].Index)
		}
		// r.logger.Printf("[DEBUG] fast update: apply indexes %v\n", req.ApplyIndexes)
	}
	return nil
}

// setPreviousLog is used to setup the PrevLogEntry and PrevLogTerm for an
// AppendEntriesRequest given the next index to replicate.
func (r *Raft) setPreviousLog(req *AppendEntriesRequest, nextIndex uint64) error {
	// Guard for the first index, since there is no 0 log entry
	// Guard against the previous index being a snapshot as well
	lastSnapIdx, lastSnapTerm := r.getLastSnapshot()
	if nextIndex == 1 {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0

	} else if (nextIndex - 1) == lastSnapIdx {
		req.PrevLogEntry = lastSnapIdx
		req.PrevLogTerm = lastSnapTerm

	} else {
		var l Log
		if err := r.logs.GetLog(nextIndex-1, &l); err != nil {
			r.logger.Printf("[ERR] raft: Failed to get log at index %d: %v",
				nextIndex-1, err)
			return err
		}

		// Set the previous index and term (0 if nextIndex is 1)
		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	}
	return nil
}

// setNewLogs is used to setup the logs which should be appended for a request.
func (r *Raft) setNewLogs(req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	// Append up to MaxAppendEntries or up to the lastIndex
	req.Entries = make([]*Log, 0, r.conf.MaxAppendEntries)
	maxIndex := min(nextIndex+uint64(r.conf.MaxAppendEntries)-1, lastIndex)
	for i := nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			r.logger.Printf("[ERR] raft: Failed to get log at index %d: %v", i, err)
			return err
		}
		req.Entries = append(req.Entries, oldLog)
	}
	return nil
}

// appendStats is used to emit stats about an AppendEntries invocation.
func appendStats(peer string, start time.Time, logs float32) {
	metrics.MeasureSince([]string{"raft", "replication", "appendEntries", "rpc", peer}, start)
	metrics.IncrCounter([]string{"raft", "replication", "appendEntries", "logs", peer}, logs)
}

// handleStaleTerm is used when a follower indicates that we have a stale term.
func (r *Raft) handleStaleTerm(s *followerReplication) {
	r.logger.Printf("[ERR] raft: peer %v has newer term, stopping replication", s.peer)
	s.notifyAll(false) // No longer leader
	asyncNotifyCh(s.stepDown)
}

// updateLastAppended is used to update follower replication state after a
// successful AppendEntries RPC.
// TODO: This isn't used during InstallSnapshot, but the code there is similar.
func updateLastAppended(s *followerReplication, req *AppendEntriesRequest) {
	// Mark any inflight logs as committed
	if logs := req.Entries; len(logs) > 0 {
		last := logs[len(logs)-1]
		s.nextIndex = last.Index + 1
		s.commitment.match(s.peer.ID, last.Index)
	}

	// Notify still leader
	s.notifyAll(true)
}

// craft
func updateLastAppendedTime(s *followerReplication, resp *AppendEntriesResponse) {
	s.timeCommitment.match(s.peer.ID, resp.Timestamp)
}

// craft
func (r *Raft) stepDown(s *followerReplication) {
	r.logger.Printf("[DEBUG] raft: stepping down from leader\n")
	s.notifyAll(false) // No longer leader
	asyncNotifyCh(s.stepDown)
}

// craft
func (r *Raft) handleFastUpdate(s *followerReplication, req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	// r.logger.Printf("[DEBUG] fast update: handle fast update from peer %v\n", s.peer.ID)
	if len(resp.LocalTerms) != r.nGroups || len(r.fastUpdateInfo) != r.nGroups {
		return
	}

	respPeerID := s.peer.ID
	for i, replica := range r.localReplicas {
		// r.logger.Printf("[DEBUG] #### fast update: from peer %v group %v, ts %v\n",
		// 	respPeerID, i, formatTimestamp(resp.NextSafeTimes[i]))
		// skip ourself
		if replica == r {
			continue
		}
		groupInfo := r.fastUpdateInfo[i]
		localTerm := replica.getCurrentTerm()

		// local info
		groupInfo[r.localID].term = localTerm
		groupInfo[r.localID].nextSafeTime = replica.nextSafeTime(req.ApplyIndexes[i])

		// resp info
		groupInfo[respPeerID].term = resp.LocalTerms[i]
		groupInfo[respPeerID].nextSafeTime = resp.NextSafeTimes[i]

		// leader must agree on the term
		if replica.leaderID == "" || groupInfo[replica.leaderID].term != localTerm {
			continue
		}

		// stale info
		if resp.NextSafeTimes[i] < groupInfo[respPeerID].nextSafeTime {
			continue
		}

		// check and update safe time if possible
		var safeTimes []int64
		for _, v := range groupInfo {
			if v.term == localTerm {
				safeTimes = append(safeTimes, v.nextSafeTime)
			}
		}

		if len(safeTimes) >= replica.quorumSize() {
			sort.Sort(int64Slice(safeTimes))
			newSafeTime := safeTimes[replica.quorumSize()-1]
			// quorum must include leader
			if groupInfo[replica.leaderID].nextSafeTime < newSafeTime {
				newSafeTime = groupInfo[replica.leaderID].nextSafeTime
			}
			// r.logger.Printf("[DEBUG] ==== fast update: group %v new safe time %v\n", i, formatTimestamp(newSafeTime))
			r.merger.UpdateSafeTime(i, newSafeTime)
		}
	}
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
