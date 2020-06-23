// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	votes := make(map[uint64]bool)
	votes[c.ID] = false
	for _, id := range c.peers {
		votes[id] = false
	}

	newRaftLog := newLog(c.Storage)
	curTem, _ := newRaftLog.Term(newRaftLog.LastIndex())

	rand := rand.New(rand.NewSource(int64(c.ID)))

	// Your Code Here (2A).
	return &Raft{
		id:               c.ID,
		Term:             curTem,
		Vote:             0,
		RaftLog:          newRaftLog,
		State:            StateFollower,
		votes:            votes,
		msgs:             []pb.Message{},
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2AB).
	return false
}

// sendHeartBeats sends a lot of heartBeats
func (r *Raft) sendHeartBeats() {
	if r.State != StateLeader {
		return
	}
	for id := range r.votes {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		// check
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// Pass the beat to the step method
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	case StateCandidate:
		fallthrough
	case StateFollower:
		r.electionElapsed++
		// check
		if r.electionElapsed == r.electionTimeout {
			// Pass the hub to the step method
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Lead = lead
	r.State = StateFollower
	r.Term = term
	r.Vote = 0

	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Vote = r.id
	r.Lead = r.id
	r.Term++
}

func (r *Raft) runElection() {
	// TODO: should a candidate be able to run twice election
	// send out the request votes
	for id := range r.votes {
		if id == r.id {
			// r.votes[id] = true
			continue
		}

		r.votes[id] = false
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
		})
	}

	// Send myself a vote as well
	r.handleVoteRequestReponse(pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
	})
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = 0

	r.heartbeatElapsed = 0
	// Append noop entry
	r.RaftLog.appendNewLog(pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// Election time out happen (2aa)
		r.becomeCandidate()
		r.runElection()
	case pb.MessageType_MsgBeat:
		// Time for heat beat (2aa)
		r.sendHeartBeats()
	case pb.MessageType_MsgPropose:
		// TODO: need to append log from local (2ab)
		if r.State == StateLeader {

		}
	case pb.MessageType_MsgAppend:
		// Handle the msg to append log (2ab)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		// TODO: reponse from append req (2ab)
	case pb.MessageType_MsgRequestVote:
		// handle request to vote (2aa)
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// handle the response of voting (2aa)
		r.handleVoteRequestReponse(m)
	case pb.MessageType_MsgSnapshot:
		// TODO: request to install a snap shot (2c)
	case pb.MessageType_MsgHeartbeat:
		// handle the heartbeat (2aa)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTransferLeader:
		// TODO:
	case pb.MessageType_MsgTimeoutNow:
		// TODO:
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2AB).
	if r.State != StateFollower {
		r.becomeFollower(m.GetTerm(), m.GetFrom())
	}
	r.Term = m.Term
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateFollower && r.Lead == m.GetFrom() {
		// Recevied heartbeat from the real leader
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.GetFrom(),
			From:    r.id,
			Term:    r.Term,
		})
	} else if r.Term < m.GetTerm() ||
		(r.Term == m.GetTerm() && r.State == StateCandidate) {
		// roll back to candidate
		r.becomeFollower(m.GetTerm(), m.GetFrom())
	} else if r.Term > m.GetTerm() {
		// pass
	} else {
		panic(
			fmt.Sprintf(
				"In the same term, there were two leaders ini the same term: %v and %v",
				m.GetFrom(),
				r.Lead,
			),
		)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleVoteRequest handles Vote RPC request
func (r *Raft) handleVoteRequest(m pb.Message) {
	var grantVote bool = false
	if r.Vote == 0 || r.Vote == m.GetFrom() {
		if r.Term > m.GetTerm() {
			grantVote = false
		} else if r.Term < m.GetTerm() {
			grantVote = true
		} else {
			if m.GetIndex() >= r.RaftLog.LastIndex() {
				grantVote = true
			}
		}
	}

	if grantVote {
		r.Vote = m.GetFrom()
		// TODO: we are not reverting as the doc sepcified
		// if r.State == StateCandidate || r.State == StateLeader {
		// 	r.becomeFollower(term)
		// }
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.GetFrom(),
		Reject:  !grantVote,
	})
}

func (r *Raft) handleVoteRequestReponse(m pb.Message) {
	if r.State != StateCandidate {
		return
	}

	r.votes[m.GetFrom()] = !m.GetReject()
	if !r.votes[m.GetFrom()] {
		return
	}

	// count the votes stupidly
	count := 0
	for _, voted := range r.votes {
		if voted {
			count++
			if count > (len(r.votes) / 2) {
				r.becomeLeader()
				return
			}
		}
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
