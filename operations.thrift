namespace go api

struct PushRequest {
    1: string producer
    2: string topic
    3: string key
    4: binary message
    5: i8 ack
}

struct PushResponse {
    1: bool ret
    2: string err
}

struct InfoRequest {
    1: string ip_port
}

struct InfoResponse {
    1: bool ret
}

//consumer准备开始接收信息
struct InfoGetRequest {
    1: string cli_name
    2: string topic_name
    3: string part_name
    4: i64 offset
    5: i8 option
}

struct InfoGetResponse {
    1: bool ret
}

struct PullRequest {
    1: string consumer
    2: string topic
    3: string key
    4: i64 offset
    5: i8 size
    6: i8 option
}

struct PullResponse {
    1: binary Msgs
    2: bool Ret
    3: i64 Start_index
    4: i64 End_index
    5: i8 Size
    6: string Err
}

//设置某个Partition接收信息的文件和队列
struct StartGetMessageRequest {
    1: string topic_name
    2: string part_name
    3: string file_name
}

struct StartGetMessageResponse {
    1: bool ret
}

//关闭某个Partition，停止接收信息
struct CloseGetMessageRequest {
    1: string topic_name
    2: string part_name
    3: string file_name
    4: string new_name
}

struct CloseGetMessageResponse {
    1: bool ret
}

//zkserver
struct PrepareAcceptRequest {
    1: string topic_name
    2: string part_name
    3: string file_name
}

struct PrepareAcceptResponse {
    1: bool ret
    2: string err
}

struct CloseAcceptRequest {
    1: string topic_name
    2: string part_name
    3: string oldfilename
    4: string newfilename
}

struct CloseAcceptResponse {
    1: bool ret
    2: i64 startindex
    3: i64 endindex
}

struct PrepareStateRequest {
    1: string TopicName
    2: string PartName
    3: i8 State
    4: binary Brokers
}

struct PrepareStateResponse {
    1: bool Ret
    2: string Err
}

struct PrepareSendRequest {
    1: string consumer
    2: string topic_name
    3: string part_name
    4: string file_name
    5: i8 option
    6: i64 offset
}

struct PrepareSendResponse {
    1: bool ret
    2: string err //若已经准备好"had_done"
}

struct AddRaftPartitionRequest {
    1: string TopicName
    2: string PartName
    3: binary Brokers
}

struct AddRaftPartitionResponse {
    1: bool Ret
    2: string Err
}

struct CloseRaftPartitionRequest {
    1: string TopicName
    2: string PartName
}

struct CloseRaftPartitionResponse {
    1: bool Ret
    2: string Err
}

struct AddFetchPartitionRequest {
    1: string TopicName
    2: string PartName
    3: string LeaderBroker
    4: string HostPort
    5: binary Brokers
    6: string FileName
}

struct AddFetchPartitionResponse {
    1: bool Ret
    2: string Err
}

struct CloseFetchPartitionRequest {
    1: string TopicName
    2: string PartName
}

struct CloseFetchPartitionResponse {
    1: bool Ret
    2: string Err
}

struct Sub2Request {
    1: string consumer
    2: string topic
    3: string key
    4: i8 option
}

struct Sub2Response {
    1: bool ret
}

service Server_Operations {
    //producer used
    PushResponse push(1: PushRequest req)

    //consumer used
    InfoResponse ConInfo(1: InfoRequest req)
    InfoGetResponse StartToGet(1: InfoGetRequest req)
    PullResponse Pull(1: PullRequest req)

    //zkserver used this rpc to request broker server
    Sub2Response Sub2(1: Sub2Request req)
    PrepareAcceptResponse PrepareAccept(1: PrepareAcceptRequest req)
    CloseAcceptResponse CloseAccept(1: CloseAcceptRequest req)
    PrepareStateResponse PrepareState(1: PrepareStateRequest req)
    PrepareSendResponse PrepareSend(1: PrepareSendRequest req)

    AddRaftPartitionResponse AddRaftPartition(1: AddRaftPartitionRequest req)
    CloseRaftPartitionResponse CloseRaftPartition(1: CloseRaftPartitionRequest req)
    AddFetchPartitionResponse AddFetchPartition(1: AddFetchPartitionRequest req)
    CloseFetchPartitionResponse CloseFetchPartition(1: CloseFetchPartitionRequest req)
}

struct CreateTopicRequest {
    1: string topic_name
}

struct CreateTopicResponse {
    1: bool ret
    2: string err
}

struct CreatePartRequest {
    1: string topic_name
    2: string part_name
}

struct CreatePartResponse {
    1: bool ret
    2: string err
}

struct SetPartitionStateRequest {
    1: string topic
    2: string part
    3: i8 option
    4: i8 repNum
}

struct SetPartitionStateResponse {
    1: bool ret
    2: string err
}

struct ProGetLeaderRequest {
    1: string topic_name
    2: string part_name
}

struct ProGetLeaderResponse {
    1: bool ret
    2: string broker_host_port
    3: string err
}

struct SubRequest {
    1: string consumer
    2: string topic
    3: string key
    4: i8 option
}

struct SubResponse {
    1: bool ret
}

struct ConStartGetBrokRequest {
    1: string cli_name
    2: string topic_name
    3: string part_name
    4: i8 option
    5: i64 index
}

struct ConStartGetBrokResponse {
    1: bool ret
    2: i64 size
    3: binary parts
}

struct BroInfoRequest {
    1: string broker_name
    2: string broker_host_port
}

struct BroInfoResponse {
    1: bool ret
}

struct UpdateRepRequest {
    1: string topic
    2: string part
    3: string BrokerName
    4: string BlockName
    5: i64 EndIndex
    6: bool leader
}

struct UpdateRepResponse {
    1: bool ret
}

struct UpdatePTPOffsetRequest {
    1: string topic
    2: string part
    3: i64 offset
}

struct UpdatePTPOffsetResponse {
    1: bool ret
}

struct BecomeLeaderRequest {
    1: string Broker
    2: string Topic
    3: string Partition
}

struct BecomeLeaderResponse {
    1: bool ret
}

struct GetNewLeaderRequest {
    1: string TopicName
    2: string PartName
    3: string BlockName
}

struct GetNewLeaderResponse {
    1: bool Ret
    2: string LeaderBroker
    3: string HostPort
}

service ZkServer_Operations {
    //produce
    CreateTopicResponse CreateTopic(1: CreateTopicRequest req)
    CreatePartResponse CreatePart(1: CreatePartRequest req)
    SetPartitionStateResponse SetPartitionState(1: SetPartitionStateRequest req)
    ProGetLeaderResponse ProGetLeader(1: ProGetLeaderRequest req)

    //consumer
    SubResponse Sub(1: SubRequest req)
    ConStartGetBrokResponse ConStartGetBroker(1: ConStartGetBrokRequest req)

    //broker
    BroInfoResponse BroInfo(1: BroInfoRequest req) //broker发送info让zkserver连接broker
    //broker更新topic-partition的offset
    UpdateRepResponse UpdateRep(1: UpdateRepRequest req)
    UpdatePTPOffsetResponse UpdatePTPOffset(1: UpdatePTPOffsetRequest req)
    //broker成为新的leader
    BecomeLeaderResponse BecomeLeader(1: BecomeLeaderRequest req)
    //broker获取新的leader
    GetNewLeaderResponse GetNewLeader(1: GetNewLeaderRequest req)
}

struct PubRequest {
    1: string topic_name
    2: string part_name
    3: i64 start_index
    4: i64 end_index
    5: binary msg
}

struct PubResponse {
    1: bool ret
}

struct PingPongRequest {
    1: bool ping
}

struct PingPongResponse {
    1: bool pong
}

service Client_Operations {
    PubResponse pub(1: PubRequest req)
    PingPongResponse pingpong(1: PingPongRequest req)
}