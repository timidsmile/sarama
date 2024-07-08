package sarama

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

// ErrClosedConsumerGroup is the error returned when a method is called on a consumer group that has been closed.
var ErrClosedConsumerGroup = errors.New("kafka: tried to use a consumer group that was closed")

// ConsumerGroup is responsible for dividing up processing of topics and partitions
// over a collection of processes (the members of the consumer group).
type ConsumerGroup interface {
	// Consume joins a cluster of consumers for a given list of topics and
	// starts a blocking ConsumerGroupSession through the ConsumerGroupHandler.
	//
	// The life-cycle of a session is represented by the following steps:
	//
	// 1. The consumers join the group (as explained in https://kafka.apache.org/documentation/#intro_consumers)
	//    and is assigned their "fair share" of partitions, aka 'claims'.
	// 2. Before processing starts, the handler's Setup() hook is called to notify the user
	//    of the claims and allow any necessary preparation or alteration of state.
	// 3. For each of the assigned claims the handler's ConsumeClaim() function is then called
	//    in a separate goroutine which requires it to be thread-safe. Any state must be carefully protected
	//    from concurrent reads/writes.
	// 4. The session will persist until one of the ConsumeClaim() functions exits. This can be either when the
	//    parent context is canceled or when a server-side rebalance cycle is initiated.
	// 5. Once all the ConsumeClaim() loops have exited, the handler's Cleanup() hook is called
	//    to allow the user to perform any final tasks before a rebalance.
	// 6. Finally, marked offsets are committed one last time before claims are released.
	//
	// Please note, that once a rebalance is triggered, sessions must be completed within
	// Config.Consumer.Group.Rebalance.Timeout. This means that ConsumeClaim() functions must exit
	// as quickly as possible to allow time for Cleanup() and the final offset commit. If the timeout
	// is exceeded, the consumer will be removed from the group by Kafka, which will cause offset
	// commit failures.
	// This method should be called inside an infinite loop, when a
	// server-side rebalance happens, the consumer session will need to be
	// recreated to get the new claims.
	Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error

	// Errors returns a read channel of errors that occurred during the consumer life-cycle.
	// By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan error

	// Close stops the ConsumerGroup and detaches any running sessions. It is required to call
	// this function before the object passes out of scope, as it will otherwise leak memory.
	Close() error

	// Pause suspends fetching from the requested partitions. Future calls to the broker will not return any
	// records from these partitions until they have been resumed using Resume()/ResumeAll().
	// Note that this method does not affect partition subscription.
	// In particular, it does not cause a group rebalance when automatic assignment is used.
	Pause(partitions map[string][]int32)

	// Resume resumes specified partitions which have been paused with Pause()/PauseAll().
	// New calls to the broker will return records from these partitions if there are any to be fetched.
	Resume(partitions map[string][]int32)

	// Pause suspends fetching from all partitions. Future calls to the broker will not return any
	// records from these partitions until they have been resumed using Resume()/ResumeAll().
	// Note that this method does not affect partition subscription.
	// In particular, it does not cause a group rebalance when automatic assignment is used.
	PauseAll()

	// Resume resumes all partitions which have been paused with Pause()/PauseAll().
	// New calls to the broker will return records from these partitions if there are any to be fetched.
	ResumeAll()
}

type consumerGroup struct {
	client Client

	config          *Config
	consumer        Consumer
	groupID         string
	groupInstanceId *string
	memberID        string
	errors          chan error

	lock       sync.Mutex
	errorsLock sync.RWMutex
	closed     chan none
	closeOnce  sync.Once

	userData []byte

	metricRegistry metrics.Registry
}

// NewConsumerGroup creates a new consumer group the given broker addresses and configuration.
// 线上web服务当前都是用的这种方式，一个服务，多个pod，用一个groupID消费一组 broker 地址
// consumerGroup 初始化ok后，通过 Consume 来消费指定的topic
func NewConsumerGroup(addrs []string, groupID string, config *Config) (ConsumerGroup, error) {
	// 初始化 client，这里主要是开启了一个后台任务来更新metadata（默认10min更新一次）
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	// 初始化 consumerGroup
	c, err := newConsumerGroup(groupID, client)
	if err != nil {
		_ = client.Close()
	}
	return c, err
}

// NewConsumerGroupFromClient creates a new consumer group using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
// PLEASE NOTE: consumer groups can only re-use but not share clients.
func NewConsumerGroupFromClient(groupID string, client Client) (ConsumerGroup, error) {
	if client == nil {
		return nil, ConfigurationError("client must not be nil")
	}
	// For clients passed in by the client, ensure we don't
	// call Close() on it.
	cli := &nopCloserClient{client}
	return newConsumerGroup(groupID, cli)
}

func newConsumerGroup(groupID string, client Client) (ConsumerGroup, error) {
	config := client.Config()
	if !config.Version.IsAtLeast(V0_10_2_0) {
		return nil, ConfigurationError("consumer groups require Version to be >= V0_10_2_0")
	}

	// 只有初始化，里面没有赋值
	consumer, err := newConsumer(client)
	if err != nil {
		return nil, err
	}

	cg := &consumerGroup{
		client:         client,
		consumer:       consumer,
		config:         config,
		groupID:        groupID,
		errors:         make(chan error, config.ChannelBufferSize),
		closed:         make(chan none),
		userData:       config.Consumer.Group.Member.UserData,
		metricRegistry: newCleanupRegistry(config.MetricRegistry),
	}
	if config.Consumer.Group.InstanceId != "" && config.Version.IsAtLeast(V2_3_0_0) {
		cg.groupInstanceId = &config.Consumer.Group.InstanceId
	}
	return cg, nil
}

// Errors implements ConsumerGroup.
func (c *consumerGroup) Errors() <-chan error { return c.errors }

// Close implements ConsumerGroup.
func (c *consumerGroup) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.closed)

		// leave group
		if e := c.leave(); e != nil {
			err = e
		}

		go func() {
			c.errorsLock.Lock()
			defer c.errorsLock.Unlock()
			close(c.errors)
		}()

		// drain errors
		for e := range c.errors {
			err = e
		}

		if e := c.client.Close(); e != nil {
			err = e
		}

		c.metricRegistry.UnregisterAll()
	})
	return
}

// Consume implements ConsumerGroup.
func (c *consumerGroup) Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error {
	// Ensure group is not closed
	select {
	case <-c.closed:
		return ErrClosedConsumerGroup
	default:
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Quick exit when no topics are provided
	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}

	// Refresh metadata for requested topics
	// 这里是启动的时候主动获取了一次 metadata（后面才是利用定时任务来获取）
	// 指定了消费的topic，获取这些topic关联的 partition 信息
	if err := c.client.RefreshMetadata(topics...); err != nil {
		return err
	}

	// Init session todo
	// 最大重试4次
	sess, err := c.newSession(ctx, topics, handler, c.config.Consumer.Group.Rebalance.Retry.Max)
	if errors.Is(err, ErrClosedClient) {
		return ErrClosedConsumerGroup
	} else if err != nil {
		return err
	}

	// Wait for session exit signal or Close() call
	// 会阻塞在这里，知道 consumerGroup 关闭，或者 session 超时
	select {
	case <-c.closed:
	case <-sess.ctx.Done():
	}

	// Gracefully release session claims
	// 消费者退出的时候，释放资源
	return sess.release(true)
}

// Pause implements ConsumerGroup.
func (c *consumerGroup) Pause(partitions map[string][]int32) {
	c.consumer.Pause(partitions)
}

// Resume implements ConsumerGroup.
func (c *consumerGroup) Resume(partitions map[string][]int32) {
	c.consumer.Resume(partitions)
}

// PauseAll implements ConsumerGroup.
func (c *consumerGroup) PauseAll() {
	c.consumer.PauseAll()
}

// ResumeAll implements ConsumerGroup.
func (c *consumerGroup) ResumeAll() {
	c.consumer.ResumeAll()
}

func (c *consumerGroup) retryNewSession(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int, refreshCoordinator bool) (*consumerGroupSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, ErrClosedConsumerGroup
	case <-time.After(c.config.Consumer.Group.Rebalance.Retry.Backoff):
		// retry 进来后会阻塞，知道满足了retry的 backoff 时间
	}

	if refreshCoordinator {
		err := c.client.RefreshCoordinator(c.groupID)
		if err != nil {
			if retries <= 0 {
				return nil, err
			}
			return c.retryNewSession(ctx, topics, handler, retries-1, true)
		}
	}

	return c.newSession(ctx, topics, handler, retries-1)
}

func (c *consumerGroup) newSession(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int) (*consumerGroupSession, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	/*
		1. 获得 coordinator
		由于一个kafka 集群中往往有多个broker，这一步是consumerGroup请求获取一个broker 作为 coordinator（协调者）
		consumerGroup里的第一个消费者请求kafka集群的时候，coordinator 就确定了。
		coordinator一般选择kafka集群里负载较低的一个broker。
	*/
	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		if retries <= 0 {
			return nil, err
		}

		//  和 newSession 的区别就是 retryNewSession 会先阻塞一个 retry backoff 的时间，然后再次调用 newSession
		return c.retryNewSession(ctx, topics, handler, retries, true)
	}

	var (
		metricRegistry          = c.metricRegistry
		consumerGroupJoinTotal  metrics.Counter
		consumerGroupJoinFailed metrics.Counter
		consumerGroupSyncTotal  metrics.Counter
		consumerGroupSyncFailed metrics.Counter
	)

	if metricRegistry != nil {
		consumerGroupJoinTotal = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-join-total-%s", c.groupID), metricRegistry)
		consumerGroupJoinFailed = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-join-failed-%s", c.groupID), metricRegistry)
		consumerGroupSyncTotal = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-sync-total-%s", c.groupID), metricRegistry)
		consumerGroupSyncFailed = metrics.GetOrRegisterCounter(fmt.Sprintf("consumer-group-sync-failed-%s", c.groupID), metricRegistry)
	}

	// Join consumer group
	/*
		这一步是所有消费者请求 coordinator 进行 leader的选举。
		这一步是阻塞的， coordinator 会在所有消费者请求完成后，根据一定策略选取一个消费者作为consumer。
		同时，coordinator 会将所有成员信息和订阅信息发送给 leader（包括消费者唯一编号 memberID，leaderID等信息）。

		一定要注意 joinGroup 这部是阻塞的，只有broker收到 consumerGroup 里的所有消费者的 joinGroup 请求时，
		coordinator 才会选择一个leader，给每个消费者分配一个唯一编号memberID，并返回 consumerGroup 里的信息给所有的消费者
	*/
	join, err := c.joinGroupRequest(coordinator, topics)
	if consumerGroupJoinTotal != nil {
		consumerGroupJoinTotal.Inc(1)
	}
	if err != nil {
		_ = coordinator.Close()
		if consumerGroupJoinFailed != nil {
			consumerGroupJoinFailed.Inc(1)
		}
		return nil, err
	}
	if !errors.Is(join.Err, ErrNoError) {
		if consumerGroupJoinFailed != nil {
			consumerGroupJoinFailed.Inc(1)
		}
	}
	switch join.Err {
	case ErrNoError:
		// join 返回成功。当前消费者获取到了一个唯一的编号
		c.memberID = join.MemberId
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		// 不识别的memberID 或者 非法的generation（表明本次返回数据有问题，需要消费者重新发起join操作）
		// 这类操作可以立马重试，不需要等待
		return c.newSession(ctx, topics, handler, retries)
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		if retries <= 0 {
			return nil, join.Err
		}
		// 这类错误（比如，正在rebalace中）表明应该要稍后重试（立马重试意义不大，因为正在忙）
		return c.retryNewSession(ctx, topics, handler, retries, true)
	case ErrMemberIdRequired:
		// from JoinGroupRequest v4 onwards (due to KIP-394) if the client starts
		// with an empty member id, it needs to get the assigned id from the
		// response and send another join request with that id to actually join the
		// group
		/*
			从 JoinGroupRequest v4 开始（由于 KIP-394），
			如果客户端以一个空成员 ID 开始，它需要从响应中获取分配的 ID，并用该 ID 发送另一个加入请求，以实际加入该组
			这里直接调用 newSession 就可以了，因为不需要等待一个backoff的时间
		*/
		c.memberID = join.MemberId
		return c.newSession(ctx, topics, handler, retries)
	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return nil, join.Err
	default:
		return nil, join.Err
	}

	/*
		确定 rebalace 的策略
	*/
	var strategy BalanceStrategy
	var ok bool
	if strategy = c.config.Consumer.Group.Rebalance.Strategy; strategy == nil {
		// 老的配置 strategy 的方法，如果业务没有指定的话，就用新的配置在获取一次
		// 新的方法支持配置多个策略，具体用哪一个，需要根据 joinGroup 之后，coordinator 返回的 GroupProtocol 来匹配
		strategy, ok = c.findStrategy(join.GroupProtocol, c.config.Consumer.Group.Rebalance.GroupStrategies)
		if !ok {
			// this case shouldn't happen in practice, since the leader will choose the protocol
			// that all the members support
			return nil, fmt.Errorf("unable to find selected strategy: %s", join.GroupProtocol)
		}
	}

	/*
		下一步：leader生成 balance 计划
		这里leader 会根据初始化时指定的 rebalance 策略生成 balance 计划，
		嘴周生成的plan 是一个map ： `memberID -> topic -> partitions` ，
		表示 每个消费者（memberID）对哪个 topic 应该消费哪些 partition
	*/

	// Prepare distribution plan if we joined as the leader
	var plan BalanceStrategyPlan
	var members map[string]ConsumerGroupMemberMetadata
	var allSubscribedTopicPartitions map[string][]int32
	var allSubscribedTopics []string
	if join.LeaderId == join.MemberId {
		// 是leader
		// 获取上一步 joinGroup 后，协调者返回的 所有成员的信息
		members, err = join.GetMembers()
		if err != nil {
			return nil, err
		}

		// 开始生成plan
		allSubscribedTopicPartitions, allSubscribedTopics, plan, err = c.balance(strategy, members)
		if err != nil {
			return nil, err
		}
	}

	// Sync consumer group
	/*
		不管是leader还是非leader 都会请求 coordinator，不同的是 leader 的请求信息中会包含完整的 plan,非leader的请求体中plan为空。
		通过 syncGroup 这个过程，coordinator 将 leader 指定的消费计划 返回给每个消费者

		每个 partition 与 consumer 的分配关系称作一个 "claim"，claim  是一个map：
		` topic -> partitions` ，表示该消费者对哪个topic应该消费哪些 partition

		消费者在 joinGroup 时已获得自己的memberID，所以根据 memberID 从plan 中取出自己的执行计划即可：
		topicA要消费partitionX，topicB要消费 partionY。
		这样后续就到了 consumer 阶段。

		注意：这里只有broker收到来自leader的syncGroup后才会对消费者进行响应。
	*/

	/*
		coordinator： 第一阶段获取到的协调者（brokerID）
		members：joinGroup步骤中，coordinator 返回的信息（是为了回传吗？）
		plan： 第三阶段生成的计划，如果不是leader，plan就为空
		generation：joinGroup阶段，coordinator 返回的第几代
		strategy：最终采用的 rebalace 策略
	*/
	syncGroupResponse, err := c.syncGroupRequest(coordinator, members, plan, join.GenerationId, strategy)
	if consumerGroupSyncTotal != nil {
		consumerGroupSyncTotal.Inc(1)
	}
	if err != nil {
		_ = coordinator.Close()
		if consumerGroupSyncFailed != nil {
			consumerGroupSyncFailed.Inc(1)
		}
		return nil, err
	}
	if !errors.Is(syncGroupResponse.Err, ErrNoError) {
		if consumerGroupSyncFailed != nil {
			consumerGroupSyncFailed.Inc(1)
		}
	}

	// syncGroup 错误处理
	switch syncGroupResponse.Err {
	case ErrNoError:
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		return c.newSession(ctx, topics, handler, retries)
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		if retries <= 0 {
			return nil, syncGroupResponse.Err
		}
		return c.retryNewSession(ctx, topics, handler, retries, true)
	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return nil, syncGroupResponse.Err
	default:
		return nil, syncGroupResponse.Err
	}

	// Retrieve and sort claims
	// 获得分配的claim，并排序
	var claims map[string][]int32
	if len(syncGroupResponse.MemberAssignment) > 0 {
		members, err := syncGroupResponse.GetMemberAssignment()
		if err != nil {
			return nil, err
		}
		claims = members.Topics

		// in the case of stateful balance strategies, hold on to the returned
		// assignment metadata, otherwise, reset the statically defined consumer
		// group metadata
		if members.UserData != nil {
			c.userData = members.UserData
		} else {
			c.userData = c.config.Consumer.Group.Member.UserData
		}

		for _, partitions := range claims {
			sort.Sort(int32Slice(partitions))
		}
	}

	/*

	 */
	session, err := newConsumerGroupSession(ctx, c, claims, join.MemberId, join.GenerationId, handler)
	if err != nil {
		return nil, err
	}

	// only the leader needs to check whether there are newly-added partitions in order to trigger a rebalance
	if join.LeaderId == join.MemberId {
		// 如果是leader，需要开启一个后台协程，用于检测是 partition 是否有变化
		go c.loopCheckPartitionNumbers(allSubscribedTopicPartitions, allSubscribedTopics, session)
	}

	return session, err
}

func (c *consumerGroup) joinGroupRequest(coordinator *Broker, topics []string) (*JoinGroupResponse, error) {
	req := &JoinGroupRequest{
		// 消费者组
		GroupId: c.groupID,
		// 第一次请求的时候，还没有 memberID。joinGroup 主要是发送 groupID 和 topic信息
		MemberId: c.memberID,
		// 默认是10s
		// coordinator 在 SessionTimeout 之后，如果没有收到 consumer 发送的心跳，就认为 consumer 挂了
		SessionTimeout: int32(c.config.Consumer.Group.Session.Timeout / time.Millisecond),
		ProtocolType:   "consumer",
	}
	if c.config.Version.IsAtLeast(V0_10_1_0) {
		req.Version = 1
		req.RebalanceTimeout = int32(c.config.Consumer.Group.Rebalance.Timeout / time.Millisecond)
	}
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 2
	}
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 2
	}
	if c.config.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 3
	}
	// from JoinGroupRequest v4 onwards (due to KIP-394) the client will actually
	// send two JoinGroupRequests, once with the empty member id, and then again
	// with the assigned id from the first response. This is handled via the
	// ErrMemberIdRequired case.
	if c.config.Version.IsAtLeast(V2_2_0_0) {
		req.Version = 4
	}
	if c.config.Version.IsAtLeast(V2_3_0_0) {
		req.Version = 5
		req.GroupInstanceId = c.groupInstanceId
	}

	meta := &ConsumerGroupMemberMetadata{
		Topics:   topics,
		UserData: c.userData,
	}
	var strategy BalanceStrategy
	// todo 为什么要把 rebalace 的策略发送给broker？ 不是client负责生成rebalace计划么，broker要这个字段是为了干啥？
	if strategy = c.config.Consumer.Group.Rebalance.Strategy; strategy != nil {
		if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
			return nil, err
		}
	} else {
		for _, strategy = range c.config.Consumer.Group.Rebalance.GroupStrategies {
			if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
				return nil, err
			}
		}
	}

	// 发起请求
	// 返回的几个关键信息：generation、memberID、leaderID、groupMembers
	return coordinator.JoinGroup(req)
}

// findStrategy returns the BalanceStrategy with the specified protocolName
// from the slice provided.
func (c *consumerGroup) findStrategy(name string, groupStrategies []BalanceStrategy) (BalanceStrategy, bool) {
	for _, strategy := range groupStrategies {
		if strategy.Name() == name {
			return strategy, true
		}
	}
	return nil, false
}

func (c *consumerGroup) syncGroupRequest(
	coordinator *Broker,
	members map[string]ConsumerGroupMemberMetadata,
	plan BalanceStrategyPlan,
	generationID int32,
	strategy BalanceStrategy,
) (*SyncGroupResponse, error) {
	req := &SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: generationID,
	}

	// Versions 1 and 2 are the same as version 0.
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 1
	}
	if c.config.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 2
	}
	// Starting from version 3, we add a new field called groupInstanceId to indicate member identity across restarts.
	if c.config.Version.IsAtLeast(V2_3_0_0) {
		req.Version = 3
		req.GroupInstanceId = c.groupInstanceId
	}

	for memberID, topics := range plan {
		assignment := &ConsumerGroupMemberAssignment{Topics: topics}
		userDataBytes, err := strategy.AssignmentData(memberID, topics, generationID)
		if err != nil {
			return nil, err
		}
		assignment.UserData = userDataBytes
		if err := req.AddGroupAssignmentMember(memberID, assignment); err != nil {
			return nil, err
		}
		delete(members, memberID)
	}
	// add empty assignments for any remaining members
	for memberID := range members {
		if err := req.AddGroupAssignmentMember(memberID, &ConsumerGroupMemberAssignment{}); err != nil {
			return nil, err
		}
	}

	return coordinator.SyncGroup(req)
}

func (c *consumerGroup) heartbeatRequest(coordinator *Broker, memberID string, generationID int32) (*HeartbeatResponse, error) {
	req := &HeartbeatRequest{
		GroupId:      c.groupID,
		MemberId:     memberID,
		GenerationId: generationID,
	}

	// Version 1 and version 2 are the same as version 0.
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 1
	}
	if c.config.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 2
	}
	// Starting from version 3, we add a new field called groupInstanceId to indicate member identity across restarts.
	if c.config.Version.IsAtLeast(V2_3_0_0) {
		req.Version = 3
		req.GroupInstanceId = c.groupInstanceId
	}

	return coordinator.Heartbeat(req)
}

func (c *consumerGroup) balance(strategy BalanceStrategy, members map[string]ConsumerGroupMemberMetadata) (map[string][]int32, []string, BalanceStrategyPlan, error) {
	topicPartitions := make(map[string][]int32)
	for _, meta := range members {
		for _, topic := range meta.Topics {
			topicPartitions[topic] = nil
		}
	}

	// 该 consumerGroup下的所有 memberID 所订阅的 topic 的合集
	allSubscribedTopics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		allSubscribedTopics = append(allSubscribedTopics, topic)
	}

	// refresh metadata for all the subscribed topics in the consumer group
	// to avoid using stale metadata to assigning partitions
	// 注意，这里主动重新刷新了一次 metadata 信息（强制发起了一次远程调用来更新内存里的metadata信息）
	err := c.client.RefreshMetadata(allSubscribedTopics...)
	if err != nil {
		return nil, nil, nil, err
	}

	// 获取topic下的partition信息
	for topic := range topicPartitions {
		// 获取该topic下的详细的 partition （直接读的内存，因为上面已经强制刷新过一次了 RefreshMetadata）
		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return nil, nil, nil, err
		}
		topicPartitions[topic] = partitions
	}

	// 根据具体的策略，生成计划
	plan, err := strategy.Plan(members, topicPartitions)
	return topicPartitions, allSubscribedTopics, plan, err
}

// Leaves the cluster, called by Close.
func (c *consumerGroup) leave() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.memberID == "" {
		return nil
	}

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	// as per KIP-345 if groupInstanceId is set, i.e. static membership is in action, then do not leave group when consumer closed, just clear memberID
	if c.groupInstanceId != nil {
		c.memberID = ""
		return nil
	}
	req := &LeaveGroupRequest{
		GroupId:  c.groupID,
		MemberId: c.memberID,
	}
	if c.config.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 1
	}
	if c.config.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 2
	}
	if c.config.Version.IsAtLeast(V2_4_0_0) {
		req.Version = 3
		req.Members = append(req.Members, MemberIdentity{
			MemberId: c.memberID,
		})
	}

	resp, err := coordinator.LeaveGroup(req)
	if err != nil {
		_ = coordinator.Close()
		return err
	}

	// clear the memberID
	c.memberID = ""

	switch resp.Err {
	case ErrRebalanceInProgress, ErrUnknownMemberId, ErrNoError:
		return nil
	default:
		return resp.Err
	}
}

func (c *consumerGroup) handleError(err error, topic string, partition int32) {
	var consumerError *ConsumerError
	if ok := errors.As(err, &consumerError); !ok && topic != "" && partition > -1 {
		err = &ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       err,
		}
	}

	if !c.config.Consumer.Return.Errors {
		Logger.Println(err)
		return
	}

	c.errorsLock.RLock()
	defer c.errorsLock.RUnlock()
	select {
	case <-c.closed:
		// consumer is closed
		return
	default:
	}

	select {
	case c.errors <- err:
	default:
		// no error listener
	}
}

func (c *consumerGroup) loopCheckPartitionNumbers(allSubscribedTopicPartitions map[string][]int32, topics []string, session *consumerGroupSession) {
	if c.config.Metadata.RefreshFrequency == time.Duration(0) {
		return
	}

	defer session.cancel()

	oldTopicToPartitionNum := make(map[string]int, len(allSubscribedTopicPartitions))
	for topic, partitions := range allSubscribedTopicPartitions {
		oldTopicToPartitionNum[topic] = len(partitions)
	}

	pause := time.NewTicker(c.config.Metadata.RefreshFrequency)
	defer pause.Stop()
	for {
		if newTopicToPartitionNum, err := c.topicToPartitionNumbers(topics); err != nil {
			return
		} else {
			for topic, num := range oldTopicToPartitionNum {
				if newTopicToPartitionNum[topic] != num {
					Logger.Printf(
						"consumergroup/%s loop check partition number goroutine find partitions in topics %s changed from %d to %d\n",
						c.groupID, topics, num, newTopicToPartitionNum[topic])
					return // trigger the end of the session on exit
				}
			}
		}
		select {
		case <-pause.C:
		case <-session.ctx.Done():
			Logger.Printf(
				"consumergroup/%s loop check partition number goroutine will exit, topics %s\n",
				c.groupID, topics)
			// if session closed by other, should be exited
			return
		case <-c.closed:
			return
		}
	}
}

func (c *consumerGroup) topicToPartitionNumbers(topics []string) (map[string]int, error) {
	topicToPartitionNum := make(map[string]int, len(topics))
	for _, topic := range topics {
		if partitionNum, err := c.client.Partitions(topic); err != nil {
			Logger.Printf(
				"consumergroup/%s topic %s get partition number failed due to '%v'\n",
				c.groupID, topic, err)
			return nil, err
		} else {
			topicToPartitionNum[topic] = len(partitionNum)
		}
	}
	return topicToPartitionNum, nil
}

// --------------------------------------------------------------------

// ConsumerGroupSession represents a consumer group member session.
type ConsumerGroupSession interface {
	// Claims returns information about the claimed partitions by topic.
	Claims() map[string][]int32

	// MemberID returns the cluster member ID.
	MemberID() string

	// GenerationID returns the current generation ID.
	GenerationID() int32

	// MarkOffset marks the provided offset, alongside a metadata string
	// that represents the state of the partition consumer at that point in time. The
	// metadata string can be used by another consumer to restore that state, so it
	// can resume consumption.
	//
	// To follow upstream conventions, you are expected to mark the offset of the
	// next message to read, not the last message read. Thus, when calling `MarkOffset`
	// you should typically add one to the offset of the last consumed message.
	//
	// Note: calling MarkOffset does not necessarily commit the offset to the backend
	// store immediately for efficiency reasons, and it may never be committed if
	// your application crashes. This means that you may end up processing the same
	// message twice, and your processing should ideally be idempotent.
	MarkOffset(topic string, partition int32, offset int64, metadata string)

	// Commit the offset to the backend
	//
	// Note: calling Commit performs a blocking synchronous operation.
	Commit()

	// ResetOffset resets to the provided offset, alongside a metadata string that
	// represents the state of the partition consumer at that point in time. Reset
	// acts as a counterpart to MarkOffset, the difference being that it allows to
	// reset an offset to an earlier or smaller value, where MarkOffset only
	// allows incrementing the offset. cf MarkOffset for more details.
	ResetOffset(topic string, partition int32, offset int64, metadata string)

	// MarkMessage marks a message as consumed.
	MarkMessage(msg *ConsumerMessage, metadata string)

	// Context returns the session context.
	Context() context.Context
}

type consumerGroupSession struct {
	parent       *consumerGroup
	memberID     string
	generationID int32
	handler      ConsumerGroupHandler

	claims  map[string][]int32
	offsets *offsetManager
	ctx     context.Context
	cancel  func()

	waitGroup       sync.WaitGroup
	releaseOnce     sync.Once
	hbDying, hbDead chan none
}

func newConsumerGroupSession(ctx context.Context, parent *consumerGroup, claims map[string][]int32, memberID string, generationID int32, handler ConsumerGroupHandler) (*consumerGroupSession, error) {
	// init context
	ctx, cancel := context.WithCancel(ctx)

	// init offset manager
	offsets, err := newOffsetManagerFromClient(parent.groupID, memberID, generationID, parent.client, cancel)
	if err != nil {
		return nil, err
	}

	// init session
	/*
		session 可以理解成 一个 consumer （consumerGroup 里的一个 consumer）
	*/
	sess := &consumerGroupSession{
		parent:       parent,
		memberID:     memberID,
		generationID: generationID,
		handler:      handler,
		offsets:      offsets,
		claims:       claims,
		ctx:          ctx,
		cancel:       cancel,
		hbDying:      make(chan none),
		hbDead:       make(chan none),
	}

	// start heartbeat loop
	/*
		当前消费者如果在指定时间内没有向服务端发送心跳则被认为死亡，将触发协调器发出分区再平衡，
		此参数可以适当设置大一些，以免由于网络抖动或者垃圾收集造成触发分区再平衡，不过如果设置太大，也会造成监测到故障节点不及时

		一个 consumerGroup 开启一个 heartbeat 用于处理
	*/
	go sess.heartbeatLoop()

	// create a POM for each claim （POM： PartitionOffsetManager ）
	for topic, partitions := range claims {
		for _, partition := range partitions {
			// 给每个topic下的，分配到的每个 partition 生成一个 partition offset manager
			// 并赋值到了 offset的 poms （topic -> partition -> offset）
			pom, err := offsets.ManagePartition(topic, partition)
			if err != nil {
				_ = sess.release(false)
				return nil, err
			}

			// handle POM errors
			go func(topic string, partition int32) {
				for err := range pom.Errors() {
					// 单独开了一个协程，从 pom 里读取错误，并处理 todo 使用场景是啥？
					sess.parent.handleError(err, topic, partition)
				}
			}(topic, partition)
		}
	}

	// perform setup
	// 消费者的 setup 。一般消费者都会重写该方法。用于通知新的session要开始了
	if err := handler.Setup(sess); err != nil {
		_ = sess.release(true)
		return nil, err
	}

	// start consuming
	for topic, partitions := range claims {
		for _, partition := range partitions {
			sess.waitGroup.Add(1)

			// 这里，也就是说这个consumer 被分配到多少个 partition 就会开启多少个协程
			// 这个协程会阻塞在 consume 方法上，用于持续的从 partition 拉取消息并消费
			// 这里就是核心的消费方法了
			go func(topic string, partition int32) {
				defer sess.waitGroup.Done()

				// cancel the as session as soon as the first
				// goroutine exits
				defer sess.cancel()

				// consume a single topic/partition, blocking
				// 开了一个协程用来消费。consume 这个方法是阻塞的（所以才开了一个协程来消费）
				sess.consume(topic, partition)
			}(topic, partition)
		}
	}

	// 返回了 session
	return sess, nil
}

func (s *consumerGroupSession) Claims() map[string][]int32 { return s.claims }
func (s *consumerGroupSession) MemberID() string           { return s.memberID }
func (s *consumerGroupSession) GenerationID() int32        { return s.generationID }

func (s *consumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		pom.MarkOffset(offset, metadata)
	}
}

func (s *consumerGroupSession) Commit() {
	s.offsets.Commit()
}

func (s *consumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		pom.ResetOffset(offset, metadata)
	}
}

func (s *consumerGroupSession) MarkMessage(msg *ConsumerMessage, metadata string) {
	s.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, metadata)
}

func (s *consumerGroupSession) Context() context.Context {
	return s.ctx
}

func (s *consumerGroupSession) consume(topic string, partition int32) {
	// quick exit if rebalance is due
	select {
	// 初始化 consumerGroupSession 的时候传进去的 cancel context，在多个地方都有接收。
	// 在多个地方都有调用 cancel，然后 partition 的消费协程就可以退出了
	case <-s.ctx.Done():
		return
	case <-s.parent.closed:
		// consumerGroup如果被关闭，这里也会退出。
		return
	default:
	}

	// get next offset
	// 从哪里开始消费。配置中指定的：最新还是最旧
	offset := s.parent.config.Consumer.Offsets.Initial
	// 取出来了 pom （因为对象是 session)
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		// 前面已经做过offset的计算了，如果能拿到就返回。否则就返回业务指定的 newest 或 oldest
		offset, _ = pom.NextOffset()
	}

	// create new claim
	claim, err := newConsumerGroupClaim(s, topic, partition, offset)
	if err != nil {
		s.parent.handleError(err, topic, partition)
		return
	}

	// handle errors
	go func() {
		for err := range claim.Errors() {
			s.parent.handleError(err, topic, partition)
		}
	}()

	// trigger close when session is done
	go func() {
		select {
		case <-s.ctx.Done():
		case <-s.parent.closed:
		}
		claim.AsyncClose()
	}()

	// start processing
	if err := s.handler.ConsumeClaim(s, claim); err != nil {
		s.parent.handleError(err, topic, partition)
	}

	// ensure consumer is closed & drained
	claim.AsyncClose()
	for _, err := range claim.waitClosed() {
		s.parent.handleError(err, topic, partition)
	}
}

func (s *consumerGroupSession) release(withCleanup bool) (err error) {
	// signal release, stop heartbeat
	s.cancel()

	// wait for consumers to exit
	// 这个ADD 是在开协程给每个partition 消费的时候
	// Done 是在每个消费协程退出的时候
	// 也就是说，这个 release 会等待每个消费协程的退出
	s.waitGroup.Wait()

	// perform release
	s.releaseOnce.Do(func() {
		if withCleanup {
			if e := s.handler.Cleanup(s); e != nil {
				s.parent.handleError(e, "", -1)
				err = e
			}
		}

		if e := s.offsets.Close(); e != nil {
			err = e
		}

		close(s.hbDying)
		<-s.hbDead
	})

	Logger.Printf(
		"consumergroup/session/%s/%d released\n",
		s.MemberID(), s.GenerationID())

	return
}

func (s *consumerGroupSession) heartbeatLoop() {
	defer close(s.hbDead)
	defer s.cancel() // trigger the end of the session on exit
	defer func() {
		Logger.Printf(
			"consumergroup/session/%s/%d heartbeat loop stopped\n",
			s.MemberID(), s.GenerationID())
	}()

	pause := time.NewTicker(s.parent.config.Consumer.Group.Heartbeat.Interval)
	defer pause.Stop()

	retryBackoff := time.NewTimer(s.parent.config.Metadata.Retry.Backoff)
	defer retryBackoff.Stop()

	retries := s.parent.config.Metadata.Retry.Max
	for {
		coordinator, err := s.parent.client.Coordinator(s.parent.groupID)
		if err != nil {
			if retries <= 0 {
				s.parent.handleError(err, "", -1)
				return
			}
			retryBackoff.Reset(s.parent.config.Metadata.Retry.Backoff)
			select {
			case <-s.hbDying:
				return
			case <-retryBackoff.C:
				retries--
			}
			continue
		}

		resp, err := s.parent.heartbeatRequest(coordinator, s.memberID, s.generationID)
		if err != nil {
			_ = coordinator.Close()

			if retries <= 0 {
				s.parent.handleError(err, "", -1)
				return
			}

			retries--
			continue
		}

		switch resp.Err {
		case ErrNoError:
			retries = s.parent.config.Metadata.Retry.Max
		case ErrRebalanceInProgress:
			retries = s.parent.config.Metadata.Retry.Max
			s.cancel()
		case ErrUnknownMemberId, ErrIllegalGeneration:
			return
		case ErrFencedInstancedId:
			if s.parent.groupInstanceId != nil {
				Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *s.parent.groupInstanceId)
			}
			s.parent.handleError(resp.Err, "", -1)
			return
		default:
			s.parent.handleError(resp.Err, "", -1)
			return
		}

		select {
		case <-pause.C:
		case <-s.hbDying:
			return
		}
	}
}

// --------------------------------------------------------------------

// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for your consumer group session life-cycle and allow you to
// trigger logic before or after the consume loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.
type ConsumerGroupHandler interface {
	// Setup is run at the beginning of a new session, before ConsumeClaim.
	Setup(ConsumerGroupSession) error

	// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
	// but before the offsets are committed for the very last time.
	Cleanup(ConsumerGroupSession) error

	// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
	// Once the Messages() channel is closed, the Handler must finish its processing
	// loop and exit.
	ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error
}

// ConsumerGroupClaim processes Kafka messages from a given topic and partition within a consumer group.
type ConsumerGroupClaim interface {
	// Topic returns the consumed topic name.
	Topic() string

	// Partition returns the consumed partition.
	Partition() int32

	// InitialOffset returns the initial offset that was used as a starting point for this claim.
	InitialOffset() int64

	// HighWaterMarkOffset returns the high watermark offset of the partition,
	// i.e. the offset that will be used for the next message that will be produced.
	// You can use this to determine how far behind the processing is.
	HighWaterMarkOffset() int64

	// Messages returns the read channel for the messages that are returned by
	// the broker. The messages channel will be closed when a new rebalance cycle
	// is due. You must finish processing and mark offsets within
	// Config.Consumer.Group.Session.Timeout before the topic/partition is eventually
	// re-assigned to another group member.
	Messages() <-chan *ConsumerMessage
}

type consumerGroupClaim struct {
	topic     string
	partition int32
	offset    int64
	PartitionConsumer
}

func newConsumerGroupClaim(sess *consumerGroupSession, topic string, partition int32, offset int64) (*consumerGroupClaim, error) {
	// 当前这个session 归属的 consumerGroup 下的 consumer ，来生成一个 pcm （那这个也是每个 partition 消费线程一个pcm了）
	pcm, err := sess.parent.consumer.ConsumePartition(topic, partition, offset)

	if errors.Is(err, ErrOffsetOutOfRange) && sess.parent.config.Consumer.Group.ResetInvalidOffsets {
		offset = sess.parent.config.Consumer.Offsets.Initial
		pcm, err = sess.parent.consumer.ConsumePartition(topic, partition, offset)
	}
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range pcm.Errors() {
			sess.parent.handleError(err, topic, partition)
		}
	}()

	return &consumerGroupClaim{
		topic:             topic,
		partition:         partition,
		offset:            offset,
		PartitionConsumer: pcm,
	}, nil
}

func (c *consumerGroupClaim) Topic() string        { return c.topic }
func (c *consumerGroupClaim) Partition() int32     { return c.partition }
func (c *consumerGroupClaim) InitialOffset() int64 { return c.offset }

// Drains messages and errors, ensures the claim is fully closed.
func (c *consumerGroupClaim) waitClosed() (errs ConsumerErrors) {
	go func() {
		for range c.Messages() {
		}
	}()

	for err := range c.Errors() {
		errs = append(errs, err)
	}
	return
}
