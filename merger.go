package craft

import (
	"container/list"
	"fmt"
	"log"
	"os"

	"math"
	"sync"
)

var (
	logger = log.New(os.Stderr, "", log.LstdFlags)
)

// MergerEntry in merger queue
type MergerEntry struct {
	Index     uint64
	GroupID   int
	Timestamp int64
	Data      []byte
	Future    *LogFuture
}

// MergerGroupLog manages log and metadata for a group
type MergerGroupLog struct {
	queue        *list.List
	safeTime     int64
	isFastUpdate bool
	// latest applied index
	applyIndex uint64
	// index -> safe time, when applying to index key,
	// safe time can be updated to corresponding value
	futureSafeTimes map[uint64]int64
}

// Merger implements log merger
type Merger struct {
	nGroups   int
	groupLogs []*MergerGroupLog
	execFunc  func(*MergerEntry)

	mergeCh chan struct{}
	lock    sync.Mutex
}

// NewMerger returns a new Merger
func NewMerger(nGroups int, execFunc func(*MergerEntry)) *Merger {
	groupLogs := make([]*MergerGroupLog, nGroups)
	for i := 0; i < nGroups; i++ {
		queue := list.New()
		groupLogs[i] = &MergerGroupLog{
			queue:           queue,
			safeTime:        0,
			isFastUpdate:    false,
			futureSafeTimes: make(map[uint64]int64),
		}
	}

	m := &Merger{
		groupLogs: groupLogs,
		nGroups:   nGroups,
		execFunc:  execFunc,
		mergeCh:   make(chan struct{}),
	}

	go m.run()

	return m
}

// UpdateSafeTime updates safe time of a given group
func (m *Merger) UpdateSafeTime(groupID int, timestamp int64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// logger.Printf("[DEBUG] merger: update safe time for group %v with %v\n", groupID,
	// 	formatTimestamp(timestamp))
	groupLog := m.groupLogs[groupID]

	if timestamp > groupLog.safeTime {
		groupLog.safeTime = timestamp
		groupLog.isFastUpdate = true
	}
	asyncNotifyCh(m.mergeCh)
}

// AddFutureSafeTime adds a future safe time update
func (m *Merger) AddFutureSafeTime(groupID int, index uint64, safeTime int64) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// logger.Printf("[DEBUG] merger: add future safe time update group %v index %v ts %v\n",
	// 	groupID, index, formatTimestamp(safeTime))

	groupLog := m.groupLogs[groupID]
	if index > groupLog.applyIndex && safeTime > groupLog.safeTime {
		groupLog.futureSafeTimes[index] = safeTime
	}
}

// Enqueue puts an entry into queue
func (m *Merger) Enqueue(groupID int, entry *MergerEntry) {
	m.lock.Lock()
	defer m.lock.Unlock()

	groupLog := m.groupLogs[groupID]

	// data len = 0 is sync entry
	if len(entry.Data) > 0 {
		groupLog.queue.PushBack(entry)
		if entry.Timestamp < groupLog.safeTime {
			panic(fmt.Sprintf("Fatal error: group %v get an entry with timestamp %v smaller than safe time %v\n",
				groupID, formatTimestamp(entry.Timestamp), formatTimestamp(groupLog.safeTime)))
		}
	}
	groupLog.applyIndex = entry.Index
	groupLog.safeTime = entry.Timestamp
	groupLog.isFastUpdate = false

	ts, ok := groupLog.futureSafeTimes[entry.Index]
	if ok {
		// logger.Printf("[DEBUG] merger: group %v use future safe time update index %v ts %v\n",
		// 	groupID, entry.Index, formatTimestamp(ts))
		groupLog.safeTime = ts
		delete(groupLog.futureSafeTimes, entry.Index)
	}

	asyncNotifyCh(m.mergeCh)

	// for i := 0; i < m.nGroups; i++ {
	// 	logger.Printf("[DEBUG] merger: group %v safe time %v\n", i, formatTimestamp(m.groupLogs[i].safeTime))
	// }
}

// GetApplyIndexes returns a list of apply indexes
func (m *Merger) GetApplyIndexes() []uint64 {
	indexes := make([]uint64, m.nGroups)
	for i := 0; i < m.nGroups; i++ {
		indexes[i] = m.groupLogs[i].applyIndex
	}
	return indexes
}

// NeedSync returns whether merging is blocked and needs a sync entry
func (m *Merger) NeedSync() bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	isFastUpdate := false
	count := 0
	for _, l := range m.groupLogs {
		if l.queue.Len() > 0 {
			count++
		}
		isFastUpdate = isFastUpdate || l.isFastUpdate
	}

	// need a sync entry if there are some entries that might not be able to merge
	return (count > 0 && count < m.nGroups)
}

// run is a long running goroutine that merges logs
func (m *Merger) run() {
	for {
		m.lock.Lock()
		group := m.nextMerge()
		if group < 0 {
			m.lock.Unlock()
			<-m.mergeCh
			continue
		}

		queue := m.groupLogs[group].queue
		e := queue.Front()
		if e == nil {
			panic("Fatal error")
		}
		entry := queue.Remove(e).(*MergerEntry)
		m.lock.Unlock()

		m.execFunc(entry)
	}
}

// nextMerge finds the next entry to be merged
// must hold the lock
func (m *Merger) nextMerge() int {
	group := -1
	var minTimestamp int64 = math.MaxInt64
	var minSafeTime int64 = math.MaxInt64

	for i := 0; i < m.nGroups; i++ {
		e := m.groupLogs[i].queue.Front()
		if e != nil {
			entry := e.Value.(*MergerEntry)
			if entry.Timestamp < minTimestamp {
				minTimestamp = entry.Timestamp
				group = i
			}
		}

		if m.groupLogs[i].safeTime < minSafeTime {
			minSafeTime = m.groupLogs[i].safeTime
		}
	}

	// logger.Printf("[DEBUG] merger: min ts %v, min safe time %v\n", formatTimestamp(minTimestamp),
	// 	formatTimestamp(minSafeTime))

	if minTimestamp > minSafeTime {
		group = -1
	}

	return group
}
