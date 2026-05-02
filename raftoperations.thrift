namespace go api

struct RequestVoteArgs {
    1: i8 term
    2: i8 candidateId
    3: i8 lastLogIndex
    4: i8 lastLogTerm
    5: string topicName
    6: string partName
}

struct RequestVoteReply {
    1: i8 term
    2: bool voteGranted
}

struct AppendEntriesArgs {
    1: i8 term
    2: i8 leaderId
    3: i8 prevLogIndex
    4: i8 prevLogTerm
    5: binary log
    6: i8 leaderCommit
    7: i8 logIndex
    8: string topicName
    9: string partName
}

struct AppendEntriesReply {
    1: i8 term
    2: bool success
    3: i8 xTerm
    4: i8 xIndex
}

struct InstallSnapshotArgs {
    1: i8 term
    2: i8 leaderId
    3: i8 lastIncludedIndex
    4: i8 lastIncludedTerm
    5: binary snapshot
    6: string topicName
    7: string partName
}

struct InstallSnapshotReply {
    1: i8 term
    2: bool success
}

struct PingPongArgs {
    1: bool ping
}

struct PingPongReply {
    1: bool pong
}

service Raft_Operations {
    RequestVoteReply RequestVote(1: RequestVoteArgs args)
    AppendEntriesReply AppendEntries(1: AppendEntriesArgs args)
    InstallSnapshotReply InstallSnapshot(1: InstallSnapshotArgs args)
    PingPongReply Pingpongtest(1: PingPongArgs req)
}